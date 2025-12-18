"""Channel-specific workers for price monitoring."""

from .base import BaseWorker
from .falabella import FalabellaWorker
from .paris import ParisWorker
from .prochef import ProchefWorker
from .ripley import RipleyWorker
from .walmart import WalmartWorker

__all__ = [
    "BaseWorker",
    "FalabellaWorker",
    "ParisWorker",
    "ProchefWorker",
    "RipleyWorker",
    "WalmartWorker",
]
