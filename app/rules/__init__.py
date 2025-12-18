"""Business rules for price monitoring."""

from .alerts import process_new_snapshots

__all__ = ["process_new_snapshots"]
