"""Replicator error hierarchy."""

from __future__ import annotations


class ReplicatorError(Exception):
    """Base error for everything the replicator raises."""


class ConfigError(ReplicatorError):
    """Configuration could not be loaded or validated."""


class EnvelopeError(ReplicatorError):
    """A Kinesis record is not a well-formed CDC envelope."""


class NotImplementedYetError(ReplicatorError):
    """Feature deferred to a later milestone."""
