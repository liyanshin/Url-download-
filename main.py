import logging
import os
import re
import signal
import subprocess
import threading
from collections import deque
from urllib.parse import quote, urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.background import BackgroundTask

log = logging.getLogger("dl")
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------- config ----
ALLOWED = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "youtu.be",
    "instagram.com", "www.instagram.com",
}
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))   # simultaneous downloads
MAX_SECONDS = int(os.getenv("MAX_SECONDS", "900"))       # hard kill per download
RATE_LIMIT = os.getenv("RATE_LIMIT", "5/minute")         # per client IP
COOKIES_FILE = os.getenv("COOKIES_FILE")                 # optional, e.g. for Instagram
MAX_URL_LEN = 2048
CHUNK = 64 * 1024
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

MEDIA_TYPES = {"mp4": "video/mp4", "webm": "video/webm", "mkv": "video/x-matroska"}

slots = threading.BoundedSemaphore(MAX_CONCURRENT)
limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

COOKIES_FILE = os.getenv("COOKIES_FILE")                 # optional, e.g. for Instagram
_cookies_b64 = os.getenv("COOKIES_B64")                  # cookies.txt, base64-encoded
if _cookies_b64 and not COOKIES_FILE:
    COOKIES_FILE = "/tmp/cookies.txt"
    with open(COOKIES_FILE, "wb") as f:
        f.write(base64.b64decode(_cookies_b64))


# --------------------------------------------------------------- helpers ----
def validate(url: str) -> None:
    if len(url) > MAX_URL_LEN:
        raise HTTPException(400, "URL too long")
    p = urlparse(url)
    if p.scheme not in ("http", "https") or (p.hostname or "").lower() not in ALLOWED:
        raise HTTPException(400, "Unsupported URL")


def safe_name(title: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", title).strip(" .")
    return name[:100] or "video"


def resolve(url: str, fmt: str) -> tuple[str, str, list[str]]:
    """One yt-dlp call -> (title, ext, direct stream URLs)."""
    cmd = ["yt-dlp", "--no-playlist", "--no-config", "-f", fmt,
           "--print", "title", "--print", "ext", "--print", "urls",
           *COOKIE_ARGS, "--", url]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Timed out resolving the video")
    if r.returncode != 0:
        log.warning("resolve failed: %s", r.stderr[-500:])
        raise HTTPException(502, "Could not resolve this video (private, blocked, or no suitable format)")
    lines = r.stdout.splitlines()
    if len(lines) < 3:
        raise HTTPException(502, "Unexpected yt-dlp output")
    urls = [l for l in lines[2:] if l.startswith("http")]
    if not urls:
        raise HTTPException(502, "No stream URL found")
    return lines[0], lines[1].strip().lower(), urls


class Job:
    """A running subprocess with a hard timeout, drained stderr, and
    exactly-once cleanup (kill, reap, release the concurrency slot)."""

    def __init__(self, cmd: list[str]):
        self.proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            start_new_session=True)  # own process group, so we can kill children too
        self.err: deque[str] = deque(maxlen=20)
        self._lock = threading.Lock()
        self._closed = False
        threading.Thread(target=self._drain, daemon=True).start()
        self.timer = threading.Timer(MAX_SECONDS, self.close)
        self.timer.daemon = True
        self.timer.start()

    def _drain(self):  # prevents the stderr pipe from filling and deadlocking
        for line in self.proc.stderr:
            self.err.append(line.decode(errors="ignore").rstrip())

    def error_tail(self) -> str:
        return " | ".join(list(self.err)[-3:])[-300:]

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self.timer.cancel()
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()  # reap, no zombies
        slots.release()


def build_ffmpeg_cmd(urls: list[str]) -> list[str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin"]
    for u in urls:
        cmd += ["-user_agent", UA,
                "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
                "-i", u]
    if len(urls) == 2:
        cmd += ["-map", "0:v:0", "-map", "1:a:0"]
    # Fragmented MP4 so the muxer never needs to seek back (pipe-safe)
    cmd += ["-c", "copy",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-f", "mp4", "pipe:1"]
    return cmd


# ------------------------------------------------------------- endpoints ----
@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/dl")
@limiter.limit(RATE_LIMIT)
def dl(request: Request, url: str, hq: bool = False, h: int = 1080):
    """
    /dl?url=<link>             -> single-file stream (<= ~720p on YouTube)
    /dl?url=<link>&hq=1&h=1080 -> separate video+audio muxed by ffmpeg on the fly
    """
    validate(url)
    h = max(144, min(h, 2160))

    if not slots.acquire(blocking=False):
        raise HTTPException(429, "Server busy, try again shortly")

    job = None
    try:
        if hq:
            # Only streams that can be stream-copied into MP4 (H.264 + AAC)
            fmt = (f"bv*[height<={h}][vcodec^=avc1]+ba[ext=m4a]"
                   f"/b[ext=mp4][height<={h}]")
            title, _, urls = resolve(url, fmt)
            ext = "mp4"
            cmd = build_ffmpeg_cmd(urls[:2])
        else:
            title, ext, _ = resolve(url, "b[ext=mp4]/b")
            ext = ext if re.fullmatch(r"[a-z0-9]{2,5}", ext) else "mp4"
            cmd = ["yt-dlp", "--no-playlist", "--no-config",
                   "-f", "b[ext=mp4]/b", "-o", "-", *COOKIE_ARGS, "--", url]

        job = Job(cmd)
        first = job.proc.stdout.read(CHUNK)  # surface failures before sending 200
        if not first:
            msg = job.error_tail()
            log.warning("stream failed: %s", msg)
            raise HTTPException(502, "Download failed to start")
    except Exception:
        if job:
            job.close()
        else:
            slots.release()
        raise

    def gen():
        try:
            yield first
            while chunk := job.proc.stdout.read(CHUNK):
                yield chunk
        finally:
            job.close()
            job.proc.stdout.close()

    name = safe_name(title)
    headers = {
        "Content-Disposition":
            f"attachment; filename=\"video.{ext}\"; filename*=UTF-8''{quote(name)}.{ext}",
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
    }
    return StreamingResponse(
        gen(),
        media_type=MEDIA_TYPES.get(ext, "application/octet-stream"),
        headers=headers,
        background=BackgroundTask(job.close),  # backstop if the client vanishes early
    )
