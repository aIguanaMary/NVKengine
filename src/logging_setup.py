import logging
import sys


def setup_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        stream=sys.stdout,
    )
    logger = logging.getLogger("bot-imobiliaria")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Segurança: o httpx (usado internamente pelo Telegram) pode logar URLs com token.
    # Mantemos o nosso logger detalhado, mas silenciamos httpx em produção.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logger
