"""
services/circuit_breaker.py
────────────────────────────────────────────────────────────────────────────
Lightweight, thread-safe circuit breaker — zero external dependencies.

States
------
CLOSED    Normal operation. All calls pass through.
OPEN      Failure threshold exceeded. Calls fail immediately (no IO).
HALF_OPEN One probe call allowed. Success → CLOSED; failure → OPEN.

Usage
-----
    from services.circuit_breaker import get_breaker

    breaker = get_breaker("drive")
    try:
        result = breaker.call(upload_to_drive, ...)
    except CircuitOpenError:
        # fail fast — Drive is known-bad
    except Exception as e:
        # real upstream error already recorded by breaker
"""

import threading
import time
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)


class CircuitOpenError(RuntimeError):
    """Raised when a call is blocked because the circuit is OPEN."""


class CircuitBreaker:
    """
    Thread-safe circuit breaker.

    Parameters
    ----------
    name              Human-readable service name (used in log messages).
    failure_threshold Number of consecutive failures before opening.
    reset_timeout     Seconds to wait in OPEN state before allowing a probe.
    """

    CLOSED    = "CLOSED"
    OPEN      = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
    ) -> None:
        self.name              = name
        self.failure_threshold = failure_threshold
        self.reset_timeout     = reset_timeout

        self._state            = self.CLOSED
        self._failure_count    = 0
        self._last_failure_at: float = 0.0
        self._lock             = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def state(self) -> str:
        """Return the current state string (thread-safe read)."""
        with self._lock:
            return self._current_state()

    def call(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute *fn* if the circuit allows it.

        Raises
        ------
        CircuitOpenError   — circuit is OPEN and reset timeout has not elapsed.
        Exception          — any exception raised by *fn* itself.
        """
        with self._lock:
            state = self._current_state()

            if state == self.OPEN:
                logger.warning(
                    "[circuit_breaker] %s OPEN — call blocked (failures=%d)",
                    self.name, self._failure_count,
                )
                raise CircuitOpenError(
                    f"Circuit '{self.name}' is OPEN — "
                    f"too many consecutive failures ({self._failure_count}). "
                    f"Retry after {self.reset_timeout}s."
                )

            if state == self.HALF_OPEN:
                logger.info(
                    "[circuit_breaker] %s HALF_OPEN — allowing probe call", self.name
                )

        # ── Execute the call (outside the lock to avoid blocking other threads) ──
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            self._on_failure(exc)
            raise

        self._on_success()
        return result

    def reset(self) -> None:
        """Manually force the circuit to CLOSED (for testing / admin use)."""
        with self._lock:
            self._state         = self.CLOSED
            self._failure_count = 0
            logger.info("[circuit_breaker] %s manually RESET to CLOSED", self.name)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _current_state(self) -> str:
        """Must be called while holding self._lock."""
        if self._state == self.OPEN:
            elapsed = time.monotonic() - self._last_failure_at
            if elapsed >= self.reset_timeout:
                self._state = self.HALF_OPEN
                logger.info(
                    "[circuit_breaker] %s → HALF_OPEN (elapsed=%.1fs)",
                    self.name, elapsed,
                )
        return self._state

    def _on_success(self) -> None:
        with self._lock:
            if self._state != self.CLOSED:
                logger.info(
                    "[circuit_breaker] %s probe succeeded → CLOSED", self.name
                )
            self._state         = self.CLOSED
            self._failure_count = 0

    def _on_failure(self, exc: Exception) -> None:
        with self._lock:
            self._failure_count  += 1
            self._last_failure_at = time.monotonic()

            if self._failure_count >= self.failure_threshold:
                self._state = self.OPEN
                logger.error(
                    "[circuit_breaker] %s → OPEN after %d consecutive failures | last_error=%s",
                    self.name, self._failure_count, exc,
                )
            else:
                logger.warning(
                    "[circuit_breaker] %s failure %d/%d | error=%s",
                    self.name, self._failure_count, self.failure_threshold, exc,
                )


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton breakers — imported by name throughout the app.
# ─────────────────────────────────────────────────────────────────────────────

_breakers: dict[str, CircuitBreaker] = {}
_breakers_lock = threading.Lock()


def get_breaker(name: str, failure_threshold: int = 5, reset_timeout: float = 60.0) -> CircuitBreaker:
    """
    Return (or create) the singleton CircuitBreaker for *name*.
    Thread-safe.
    """
    with _breakers_lock:
        if name not in _breakers:
            _breakers[name] = CircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                reset_timeout=reset_timeout,
            )
        return _breakers[name]


def all_breaker_states() -> dict[str, str]:
    """Return a snapshot of every registered breaker's state."""
    with _breakers_lock:
        return {name: cb.state for name, cb in _breakers.items()}
