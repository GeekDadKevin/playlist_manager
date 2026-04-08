from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from flask import Flask

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def create_app(config_class: type[object] | None = None) -> Flask:
    from validate_env import validate_environment

    errors = validate_environment()
    if errors:
        joined_errors = "\n- ".join(errors)
        raise ValueError(f"Environment validation failed:\n- {joined_errors}")

    from .config import Config

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config.from_object(config_class or Config)

    Path(app.config["DATA_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["UPLOAD_FOLDER"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["NAVIDROME_PLAYLISTS_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["SETTINGS_FILE"]).parent.mkdir(parents=True, exist_ok=True)
    Path(app.config["PLAYLIST_DB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

    from .routes.api import api_bp
    from .routes.web import web_bp
    from .services.playlist_history import init_playlist_history
    from .services.scheduled_imports import start_playlist_scheduler

    init_playlist_history(app.config["PLAYLIST_DB_PATH"])
    app.register_blueprint(web_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    start_playlist_scheduler(app)

    return app


app = create_app()
