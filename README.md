# Call Analytics PoC (FreePBX/Asterisk) — Local Whisper + Ollama

## What it does
- takes FreePBX call recordings (.wav) from `calls_raw/YYYY/MM/DD/`
- normalizes to 16k mono wav
- transcribes with faster-whisper (GPU)
- translates transcript + produces analysis in Ukrainian (UA)
- outputs per-call JSON + aggregated `out/report.json`

## Run
```bash
cp .env.example .env
docker compose build whisper_poc
docker compose run --rm whisper_poc