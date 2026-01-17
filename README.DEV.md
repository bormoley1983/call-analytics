# Call Analytics Developer Manual

## Development & Deployment

- **All development and production runs are containerized.**
- Use `Dockerfile.base` and `compose.yaml` for both dev and prod environments.
- No need to install Python or dependencies locally.

### Build and Run

```bash
docker compose build
docker compose up
```

- For development, you can mount your source code and config as volumes in `compose.yaml` for live code reloads.

## Project Structure

- `src/core/`: Main business logic (pipeline, reports, rules, transcription).
- `src/adapters/`: Integrations (audio, LLM, PBX, storage, HTML reports).
- `src/domain/`: Configuration and data models.
- `src/ports/`: Abstract interfaces for adapters.
- `src/old/`: Legacy code (for reference only).

## Pipeline Overview

- The main workflow is encapsulated in `core/pipeline.py` as the `Pipeline` class.
- Entry point (`cli.py`) instantiate `Pipeline` with `AppConfig` and a storage backend.

## Key Classes & Functions

- **Pipeline:** Orchestrates the processing phases.
- **JsonStorage:** Centralized JSON file operations (`adapters/storage_json.py`).
- **Transcription:** `core/transcription.py`
- **Reports:** `core/reports.py`
- **PBX Filename Parsing:** Use `parse_filename` from the appropriate PBX adapter.

## Extending

- **Add a new PBX adapter:** Create a new module in `adapters/` and implement `parse_filename`.
- **Add a new storage backend:** Implement the storage interface in `ports/storage.py`.
- **Add new reports:** Extend `core/reports.py`.
- **Add new adapters:** in the appropriate module.
- **For testing:** add tests to a `tests/` directory and run them in the container:
  ```bash
  docker compose run --rm app pytest
  ```

## Testing

- Place tests in a `tests/` directory.
- Use `pytest` or `unittest` for new modules.

## Contribution Guidelines

- Use type hints and docstrings.
- Follow PEP8 style.
- Submit pull requests for review.