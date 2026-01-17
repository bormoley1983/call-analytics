# Call Analytics

A tool for processing, transcribing, and analyzing call recordings from FreePBX/Asterisk systems.

## Quick Start (Docker)

No installation required! Everything runs in a container.

### 1. Build the Docker image

```bash
docker compose build
```

### 2. Run the pipeline

```bash
docker compose up
```

This will process your calls and generate reports automatically.

## Configuration

- Place your configuration files in the `config/` directory before starting the container.
- Edit `config/managers.yaml`, `brands.yaml`, and `analysis.yaml` as needed.

## Output

- Results will be available in the `out/` directory (mounted from the container).

## Troubleshooting

- Make sure your audio files are in the correct location (`calls_raw/`).
- Check container logs for errors:  
  ```bash
  docker compose logs
  ```

<!-- ## OLD Run
```bash
cp .env.example .env
docker compose build whisper_poc
docker compose run --rm whisper_poc -->