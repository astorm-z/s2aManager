from __future__ import annotations


class AppError(RuntimeError):
    """应用级错误。"""


class ConfigError(AppError):
    """配置错误。"""


class APIError(AppError):
    def __init__(self, status: int, code: int | str | None, message: str, details: object | None = None) -> None:
        self.status = status
        self.code = code
        self.details = details
        super().__init__(f"HTTP {status} | code={code} | {message}")
