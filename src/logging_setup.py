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
    return logger
