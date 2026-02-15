import logging
import os
from logging.handlers import RotatingFileHandler


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"

    root = logging.getLogger()
    root.setLevel(level)

    # Keep console logs for journald/systemd capture.
    has_stream = any(isinstance(h, logging.StreamHandler) for h in root.handlers)
    if not has_stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(stream_handler)

    if not _env_bool("LOG_TO_FILE", True):
        return

    log_file = os.getenv("LOG_FILE_PATH", "/var/log/serve-ai/serve-vm-agents.log")
    max_bytes = int(os.getenv("LOG_FILE_MAX_BYTES", str(10 * 1024 * 1024)))
    backup_count = int(os.getenv("LOG_FILE_BACKUP_COUNT", "5"))

    # Avoid adding duplicate file handlers if setup_logging() is called repeatedly.
    has_file_handler = any(
        isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "") == os.path.abspath(log_file)
        for h in root.handlers
    )
    if has_file_handler:
        return

    try:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(file_handler)
    except Exception as exc:
        # Do not break app startup if file logging path is not writable.
        root.warning("File logging disabled: could not open %s (%s)", log_file, exc)