from __future__ import annotations

import uvicorn

from .main import app
from .settings import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(app, host=settings.app.host, port=settings.app.port)


if __name__ == "__main__":
    main()
