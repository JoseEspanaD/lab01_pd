"""Crea la configuración local de Airflow sin imprimir sus secretos."""

from __future__ import annotations

import base64
import os
import secrets
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    for name in ("state", "logs"):
        directory = root / "airflow" / name
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory.chmod(0o700)

    path = root / ".env"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        print(".env ya existe; se conserva sin cambios.")
        return

    values = {
        "AIRFLOW_UID": str(os.getuid()),
        "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(48),
        "AIRFLOW_API_SECRET_KEY": secrets.token_urlsafe(48),
        "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii"),
    }
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        for name, value in values.items():
            output.write(f"{name}={value}\n")
    print(".env creado con permisos privados; secretos omitidos de la salida.")


if __name__ == "__main__":
    main()
