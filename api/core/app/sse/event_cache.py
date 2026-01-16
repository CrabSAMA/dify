"""
SSE Event Cache Service

Provides Redis-based caching for SSE events to support client reconnection
via the Last-Event-ID header.
"""

import logging
from typing import NamedTuple

from configs import dify_config
from extensions.ext_redis import redis_client

logger = logging.getLogger(__name__)

# Redis key prefix for SSE event cache
SSE_EVENT_CACHE_PREFIX = "sse:events:"


class CachedSSEEvent(NamedTuple):
    """Represents a cached SSE event."""

    event_id: str
    data: str


class SSEEventCache:
    """
    Redis-based cache for SSE events to support reconnection.

    Events are stored in a Redis List with the format:
    - Key: sse:events:{task_id}
    - Value: JSON-encoded list of {id, data} objects

    Each event ID follows the format: {task_id}:{sequence_number}
    """

    def __init__(self, task_id: str):
        """
        Initialize the SSE event cache for a specific task.

        Args:
            task_id: The unique task identifier for the SSE stream.
        """
        self._task_id = task_id
        self._cache_key = f"{SSE_EVENT_CACHE_PREFIX}{task_id}"
        self._sequence = 0
        self._enabled = dify_config.SSE_RECONNECT_ENABLED
        self._ttl = dify_config.SSE_EVENT_CACHE_TTL
        self._max_size = dify_config.SSE_EVENT_CACHE_MAX_SIZE

    @property
    def enabled(self) -> bool:
        """Check if SSE reconnection is enabled."""
        return self._enabled

    @property
    def task_id(self) -> str:
        """Get the task ID."""
        return self._task_id

    def generate_event_id(self) -> str:
        """
        Generate a unique event ID for the current event.

        Returns:
            Event ID in the format {task_id}:{sequence_number}
        """
        self._sequence += 1
        return f"{self._task_id}:{self._sequence}"

    def push_event(self, event_id: str, data: str) -> bool:
        """
        Push an event to the cache.

        Args:
            event_id: The unique event identifier.
            data: The SSE data payload (already formatted as SSE string).

        Returns:
            True if the event was cached successfully, False otherwise.
        """
        if not self._enabled:
            return False

        try:
            # Store event as a simple format: event_id\x00data
            # Using null byte as separator since it won't appear in SSE data
            cache_entry = f"{event_id}\x00{data}"
            redis_client.rpush(self._cache_key, cache_entry)

            # Set TTL on the key
            redis_client.expire(self._cache_key, self._ttl)

            # Trim the list if it exceeds max size
            list_length = redis_client.llen(self._cache_key)
            if list_length > self._max_size:
                # Keep only the most recent events
                redis_client.ltrim(self._cache_key, list_length - self._max_size, -1)

            return True
        except Exception as e:
            logger.warning("Failed to cache SSE event for task %s: %s", self._task_id, e)
            return False

    def get_events_after(self, last_event_id: str) -> list[CachedSSEEvent]:
        """
        Get all cached events after the specified event ID.

        Args:
            last_event_id: The last event ID received by the client.

        Returns:
            List of CachedSSEEvent objects that occurred after the given event ID.
        """
        if not self._enabled:
            return []

        try:
            # Parse the sequence number from the last_event_id
            parts = last_event_id.rsplit(":", 1)
            if len(parts) != 2:
                logger.warning("Invalid last_event_id format: %s", last_event_id)
                return []

            event_task_id, seq_str = parts
            if event_task_id != self._task_id:
                logger.warning(
                    "Task ID mismatch in last_event_id: expected %s, got %s", self._task_id, event_task_id
                )
                return []

            try:
                last_sequence = int(seq_str)
            except ValueError:
                logger.warning("Invalid sequence number in last_event_id: %s", seq_str)
                return []

            # Get all events from the cache
            raw_events = redis_client.lrange(self._cache_key, 0, -1)
            if not raw_events:
                return []

            # Filter events that come after the last_sequence
            result: list[CachedSSEEvent] = []
            for raw_event in raw_events:
                if isinstance(raw_event, bytes):
                    raw_event = raw_event.decode("utf-8")

                # Parse the cached entry
                separator_idx = raw_event.find("\x00")
                if separator_idx == -1:
                    continue

                event_id = raw_event[:separator_idx]
                data = raw_event[separator_idx + 1 :]

                # Parse sequence from event_id
                event_parts = event_id.rsplit(":", 1)
                if len(event_parts) != 2:
                    continue

                try:
                    event_sequence = int(event_parts[1])
                except ValueError:
                    continue

                # Only include events after the last received event
                if event_sequence > last_sequence:
                    result.append(CachedSSEEvent(event_id=event_id, data=data))

            return result

        except Exception as e:
            logger.warning("Failed to get cached SSE events for task %s: %s", self._task_id, e)
            return []

    def get_all_events(self) -> list[CachedSSEEvent]:
        """
        Get all cached events for the task.

        Returns:
            List of all CachedSSEEvent objects.
        """
        if not self._enabled:
            return []

        try:
            raw_events = redis_client.lrange(self._cache_key, 0, -1)
            if not raw_events:
                return []

            result: list[CachedSSEEvent] = []
            for raw_event in raw_events:
                if isinstance(raw_event, bytes):
                    raw_event = raw_event.decode("utf-8")

                separator_idx = raw_event.find("\x00")
                if separator_idx == -1:
                    continue

                event_id = raw_event[:separator_idx]
                data = raw_event[separator_idx + 1 :]
                result.append(CachedSSEEvent(event_id=event_id, data=data))

            return result

        except Exception as e:
            logger.warning("Failed to get all cached SSE events for task %s: %s", self._task_id, e)
            return []

    def clear(self) -> bool:
        """
        Clear all cached events for the task.

        Returns:
            True if the cache was cleared successfully, False otherwise.
        """
        if not self._enabled:
            return False

        try:
            redis_client.delete(self._cache_key)
            return True
        except Exception as e:
            logger.warning("Failed to clear SSE event cache for task %s: %s", self._task_id, e)
            return False

    def exists(self) -> bool:
        """
        Check if there are any cached events for the task.

        Returns:
            True if cached events exist, False otherwise.
        """
        if not self._enabled:
            return False

        try:
            return redis_client.exists(self._cache_key) > 0
        except Exception as e:
            logger.warning("Failed to check SSE event cache existence for task %s: %s", self._task_id, e)
            return False

    def get_event_count(self) -> int:
        """
        Get the number of cached events for the task.

        Returns:
            Number of cached events.
        """
        if not self._enabled:
            return 0

        try:
            return redis_client.llen(self._cache_key) or 0
        except Exception as e:
            logger.warning("Failed to get SSE event count for task %s: %s", self._task_id, e)
            return 0


def parse_last_event_id(last_event_id: str | None) -> tuple[str | None, int | None]:
    """
    Parse a Last-Event-ID header value into task_id and sequence number.

    Args:
        last_event_id: The Last-Event-ID header value.

    Returns:
        Tuple of (task_id, sequence) or (None, None) if invalid.
    """
    if not last_event_id:
        return None, None

    parts = last_event_id.rsplit(":", 1)
    if len(parts) != 2:
        return None, None

    task_id, seq_str = parts
    try:
        sequence = int(seq_str)
        return task_id, sequence
    except ValueError:
        return None, None


def is_sse_reconnect_enabled() -> bool:
    """
    Check if SSE reconnection support is enabled.

    Returns:
        True if SSE reconnection is enabled, False otherwise.
    """
    return dify_config.SSE_RECONNECT_ENABLED
