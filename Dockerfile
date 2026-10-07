FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# yt-dlp needs a JS runtime to solve YouTube challenges
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN python -m venv /opt/venv

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -U -r requirements.txt
COPY main.py .

RUN useradd -m app && chown -R app /opt/venv /app
USER app

EXPOSE 8000

# Update yt-dlp on every container start (sites break it often), then serve.
# --proxy-headers makes per-IP rate limiting see the real client IP behind
# Fly/Railway's proxy. Only use "*" when you're always behind a trusted proxy.
CMD ["sh", "-c", "pip install -U -q 'yt-dlp[default]' || true; exec uvicorn main:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips='*'"]
