from __future__ import annotations
import os
from dataclasses import dataclass, field
from typing import Set
from zoneinfo import ZoneInfo
from dotenv import load_dotenv


class ConfigError(Exception):
    pass


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "sim", "s"}


@dataclass
class Settings:
    telegram_bot_token: str
    sheets_webapp_url: str
    sheets_webapp_secret: str
    timezone_name: str = "America/Sao_Paulo"
    log_level: str = "INFO"
    delete_user_messages: bool = True
    pending_notice_reminder_minutes: int = 60
    pending_notice_check_every_minutes: int = 10
    admin_telegram_ids: Set[int] = field(default_factory=set)

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)


def load_settings() -> Settings:
    load_dotenv()

    missing = []
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    webapp_url = os.getenv("SHEETS_WEBAPP_URL", "").strip()
    webapp_secret = os.getenv("SHEETS_WEBAPP_SECRET", "").strip()

    if not token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not webapp_url:
        missing.append("SHEETS_WEBAPP_URL")
    if not webapp_secret:
        missing.append("SHEETS_WEBAPP_SECRET")

    if missing:
        raise ConfigError("Variáveis obrigatórias ausentes: " + ", ".join(missing))

    admin_ids_raw = os.getenv("ADMIN_TELEGRAM_IDS", "").strip()
    admin_ids: Set[int] = set()
    if admin_ids_raw:
        for part in admin_ids_raw.split(","):
            p = part.strip()
            if not p:
                continue
            try:
                admin_ids.add(int(p))
            except ValueError as exc:
                raise ConfigError(f"ADMIN_TELEGRAM_IDS inválido: '{p}' não é número.") from exc

    timezone_name = os.getenv("BOT_TIMEZONE", "America/Sao_Paulo").strip() or "America/Sao_Paulo"
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise ConfigError(f"BOT_TIMEZONE inválido: {timezone_name}") from exc

    reminder_minutes_raw = os.getenv("PENDING_NOTICE_REMINDER_MINUTES", "60").strip()
    check_every_raw = os.getenv("PENDING_NOTICE_CHECK_EVERY_MINUTES", "10").strip()
    try:
        reminder_minutes = max(5, int(reminder_minutes_raw))
        check_every = max(1, int(check_every_raw))
    except ValueError as exc:
        raise ConfigError("PENDING_NOTICE_REMINDER_MINUTES e PENDING_NOTICE_CHECK_EVERY_MINUTES devem ser números.") from exc

    return Settings(
        telegram_bot_token=token,
        sheets_webapp_url=webapp_url,
        sheets_webapp_secret=webapp_secret,
        timezone_name=timezone_name,
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        delete_user_messages=_parse_bool(os.getenv("DELETE_USER_MESSAGES"), True),
        pending_notice_reminder_minutes=reminder_minutes,
        pending_notice_check_every_minutes=check_every,
        admin_telegram_ids=admin_ids,
    )
