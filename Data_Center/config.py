from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / ".env"
DEFAULT_DB_PATH = ROOT_DIR / "Data_Center" / "Data.db"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if not ENV_PATH.exists():
        return env

    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value
        os.environ.setdefault(key, value)
    return env


def save_env_value(key: str, value: str) -> None:
    lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    replaced = False
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        name = stripped.split("=", 1)[0].strip()
        if name == key:
            lines[idx] = f"{key}={value}"
            replaced = True
            break
    if not replaced:
        lines.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def get_first_env(env: dict[str, str], names: tuple[str, ...]) -> tuple[str, str]:
    for name in names:
        value = os.environ.get(name) or env.get(name, "")
        if value:
            return name, value
    return names[0], ""


def db_path_from_env(env: dict[str, str]) -> Path:
    raw = env.get("DATA_CENTER_DB_PATH") or os.environ.get("DATA_CENTER_DB_PATH") or str(DEFAULT_DB_PATH)
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path
