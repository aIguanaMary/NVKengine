from __future__ import annotations
import copy
import re
from datetime import datetime
from typing import Any, Dict


def now_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "arquivo"


def deep_copy_session_state(session: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "state": session.get("state", "MAIN_MENU"),
        "data": copy.deepcopy(session.get("data", {})),
        "meta": copy.deepcopy(session.get("meta", {})),
        "choices": copy.deepcopy(session.get("choices", [])),
    }


def normalize_yes(value: str | None) -> bool:
    return (value or "").strip().upper() in {"SIM", "S", "TRUE", "1", "YES", "Y"}


def parse_date_br(text: str) -> str | None:
    text = text.strip()
    try:
        dt = datetime.strptime(text, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_time_hhmm(text: str) -> str | None:
    text = text.strip()
    try:
        dt = datetime.strptime(text, "%H:%M")
        return dt.strftime("%H:%M")
    except ValueError:
        return None
