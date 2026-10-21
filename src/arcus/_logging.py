"""The "arcus" logger; applications configure its handlers and level."""

import logging

logger = logging.getLogger("arcus")
logger.addHandler(logging.NullHandler())
