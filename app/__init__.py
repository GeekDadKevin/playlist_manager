from __future__ import annotations

import json
import logging
import logging.handlers
import os
from pathlib import Path

from dotenv import load_dotenv
from env_config import (
    coerce_json_config_values,
    read_json_config,
    should_apply_json_config,
)
from flask import Flask, request
from werkzeug.exceptions import HTTPException, InternalServerError

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)


def _load_json_config() -> None:
    if not should_apply_json_config():
        return

    config_path = Path(__file__).resolve().parent.parent / "config.json"
    try:
        raw = read_json_config(config_path)
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        logging.getLogger(__name__).warning("Config JSON read failed: %s", exc)
        return
    if raw is None:
        return

    for key, value in coerce_json_config_values(raw).items():
        if os.environ.get(key, ""):
            continue
        os.environ[key] = value


_load_json_config()


def _configure_logging(data_dir: str) -> None:
    log_dir = Path(data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "app.log"
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    if not any(
        isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers
    ):
        root.setLevel(logging.DEBUG)
        root.addHandler(file_handler)

    # Silence noisy third-party loggers
    for noisy in ("httpx", "httpcore", "werkzeug"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def create_app(config_class: type[object] | None = None) -> Flask:
    from validate_env import validate_environment

    errors = validate_environment()
    if errors:
        joined_errors = "\n- ".join(errors)
        raise ValueError(f"Environment validation failed:\n- {joined_errors}")

    from .config import Config

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(config_class or Config)

    playlist_dir = str(
        app.config.get("NAVIDROME_PLAYLISTS_DIR")
        or app.config.get("NAVIDROME_PLAYLIST_DIR")
        or "/navidrome/playlist"
    ).strip()
    app.config["NAVIDROME_PLAYLIST_DIR"] = playlist_dir
    app.config["NAVIDROME_PLAYLISTS_DIR"] = playlist_dir

    Path(app.config["DATA_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)
    Path(playlist_dir).mkdir(parents=True, exist_ok=True)
    Path(app.config["SETTINGS_FILE"]).parent.mkdir(parents=True, exist_ok=True)
    Path(app.config["PLAYLIST_DB_PATH"]).parent.mkdir(parents=True, exist_ok=True)
    Path(app.config["LIBRARY_INDEX_DB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

    _configure_logging(app.config["DATA_DIR"])

    from .routes.api import api_bp
    from .routes.web import web_bp
    from .services.library_index import init_library_index
    from .services.playlist_history import init_playlist_history
    from .services.scheduled_imports import start_playlist_scheduler

    init_playlist_history(app.config["PLAYLIST_DB_PATH"])
    init_library_index(app.config["LIBRARY_INDEX_DB_PATH"])
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    start_playlist_scheduler(app)

    _register_error_handlers(app)

    return app


def _register_error_handlers(app: Flask) -> None:
    log = logging.getLogger(__name__)

    def handle_http_error(error: HTTPException):
        log.warning("HTTP %s %s %s", error.code, request.method, request.path)
        return error

    def handle_unhandled_error(error: Exception):
        if isinstance(error, HTTPException):
            return handle_http_error(error)
        log.exception("Unhandled error during %s %s", request.method, request.path)
        return InternalServerError()

    app.register_error_handler(HTTPException, handle_http_error)
    app.register_error_handler(Exception, handle_unhandled_error)


app = create_app()
