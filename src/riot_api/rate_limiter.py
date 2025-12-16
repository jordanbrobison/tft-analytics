import time
import threading
from typing import Optional


class RateLimiter:
    """Token bucket rate limiter for API requests.

    Implements two rate limits simultaneously:
    - Short-term: 20 requests per 1 second
    - Long-term: 100 requests per 120 seconds
    """

    def __init__(
        self,
        short_limit: int = 20,
        short_window: float = 1.0,
        long_limit: int = 100,
        long_window: float = 120.0
    ):
        """Initialize rate limiter with dual rate limits.

        Args:
            short_limit: Maximum requests in short window (default 20)
            short_window: Short window duration in seconds (default 1.0)
            long_limit: Maximum requests in long window (default 100)
            long_window: Long window duration in seconds (default 120.0)
        """
        self.short_limit = short_limit
        self.short_window = short_window
        self.long_limit = long_limit
        self.long_window = long_window

        # Token buckets
        self.short_tokens = float(short_limit)
        self.long_tokens = float(long_limit)

        # Last refill times
        self.short_last_refill = time.time()
        self.long_last_refill = time.time()

        # Thread lock for thread-safe operations
        self._lock = threading.Lock()

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time since last refill."""
        now = time.time()

        # Refill short-term bucket
        short_elapsed = now - self.short_last_refill
        short_refill_rate = self.short_limit / self.short_window
        self.short_tokens = min(
            self.short_limit,
            self.short_tokens + short_elapsed * short_refill_rate
        )
        self.short_last_refill = now

        # Refill long-term bucket
        long_elapsed = now - self.long_last_refill
        long_refill_rate = self.long_limit / self.long_window
        self.long_tokens = min(
            self.long_limit,
            self.long_tokens + long_elapsed * long_refill_rate
        )
        self.long_last_refill = now

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """Acquire tokens for making API requests.

        Blocks until tokens are available or timeout is reached.

        Args:
            tokens: Number of tokens to acquire (default 1)
            timeout: Maximum time to wait in seconds (None = wait forever)

        Returns:
            True if tokens were acquired, False if timeout was reached
        """
        deadline = None if timeout is None else time.time() + timeout

        while True:
            with self._lock:
                self._refill_tokens()

                # Check if we have enough tokens in both buckets
                if self.short_tokens >= tokens and self.long_tokens >= tokens:
                    self.short_tokens -= tokens
                    self.long_tokens -= tokens
                    return True

            # Check timeout
            if deadline is not None and time.time() >= deadline:
                return False

            # Wait a bit before checking again
            # Wait time is based on when the limiting bucket will refill
            with self._lock:
                short_wait = (tokens - self.short_tokens) / (self.short_limit / self.short_window)
                long_wait = (tokens - self.long_tokens) / (self.long_limit / self.long_window)
                wait_time = max(0.01, min(short_wait, long_wait))

            time.sleep(wait_time)

    def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens without blocking.

        Args:
            tokens: Number of tokens to acquire (default 1)

        Returns:
            True if tokens were acquired, False otherwise
        """
        with self._lock:
            self._refill_tokens()

            if self.short_tokens >= tokens and self.long_tokens >= tokens:
                self.short_tokens -= tokens
                self.long_tokens -= tokens
                return True

            return False

    def reset(self) -> None:
        """Reset rate limiter to initial state."""
        with self._lock:
            self.short_tokens = float(self.short_limit)
            self.long_tokens = float(self.long_limit)
            self.short_last_refill = time.time()
            self.long_last_refill = time.time()

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get estimated wait time for acquiring tokens.

        Args:
            tokens: Number of tokens needed

        Returns:
            Estimated wait time in seconds
        """
        with self._lock:
            self._refill_tokens()

            if self.short_tokens >= tokens and self.long_tokens >= tokens:
                return 0.0

            short_wait = max(0, (tokens - self.short_tokens) / (self.short_limit / self.short_window))
            long_wait = max(0, (tokens - self.long_tokens) / (self.long_limit / self.long_window))

            return max(short_wait, long_wait)
