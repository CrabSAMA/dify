"""
SSE Reconnection Wrapper

Provides functionality to resume SSE streams from the last received event
when a client reconnects with a Last-Event-ID header.
"""

import logging
from collections.abc import Callable, Generator
from typing import Any

from core.app.sse.event_cache import SSEEventCache, parse_last_event_id

logger = logging.getLogger(__name__)


class SSEReconnectStatus:
    """Status constants for SSE reconnection."""

    # Reconnection successful, resuming from cached events
    RESUMED = "resumed"
    # No cached events found, starting fresh stream
    NO_CACHE = "no_cache"
    # Task ID mismatch, cannot resume
    TASK_MISMATCH = "task_mismatch"
    # Invalid Last-Event-ID format
    INVALID_ID = "invalid_id"
    # Reconnection disabled
    DISABLED = "disabled"


class SSEReconnectResult:
    """Result of SSE reconnection attempt."""

    def __init__(
        self,
        status: str,
        cached_events: list[tuple[str, str]] | None = None,
        resume_sequence: int | None = None,
    ):
        """
        Initialize reconnection result.

        Args:
            status: One of SSEReconnectStatus values.
            cached_events: List of (event_id, data) tuples to replay.
            resume_sequence: The sequence number to resume from.
        """
        self.status = status
        self.cached_events = cached_events or []
        self.resume_sequence = resume_sequence

    @property
    def can_resume(self) -> bool:
        """Check if we can resume from cached events."""
        return self.status == SSEReconnectStatus.RESUMED and len(self.cached_events) > 0

    @property
    def has_cached_events(self) -> bool:
        """Check if there are cached events to replay."""
        return len(self.cached_events) > 0


def try_reconnect(task_id: str, last_event_id: str | None) -> SSEReconnectResult:
    """
    Attempt to reconnect to an SSE stream using the Last-Event-ID.

    Args:
        task_id: The task ID for the SSE stream.
        last_event_id: The Last-Event-ID header value from the client.

    Returns:
        SSEReconnectResult with status and any cached events to replay.
    """
    cache = SSEEventCache(task_id)

    if not cache.enabled:
        return SSEReconnectResult(status=SSEReconnectStatus.DISABLED)

    if not last_event_id:
        return SSEReconnectResult(status=SSEReconnectStatus.NO_CACHE)

    # Parse the last event ID
    parsed_task_id, sequence = parse_last_event_id(last_event_id)

    if parsed_task_id is None or sequence is None:
        logger.warning("Invalid Last-Event-ID format: %s", last_event_id)
        return SSEReconnectResult(status=SSEReconnectStatus.INVALID_ID)

    if parsed_task_id != task_id:
        logger.warning(
            "Task ID mismatch in reconnection: expected %s, got %s",
            task_id,
            parsed_task_id,
        )
        return SSEReconnectResult(status=SSEReconnectStatus.TASK_MISMATCH)

    # Get cached events after the last received event
    cached_events = cache.get_events_after(last_event_id)

    if not cached_events:
        # No cached events to replay - either cache expired or no new events
        if cache.exists():
            logger.debug("No new events after %s for task %s", last_event_id, task_id)
        else:
            logger.debug("No cached events found for task %s", task_id)
        return SSEReconnectResult(
            status=SSEReconnectStatus.NO_CACHE,
            resume_sequence=sequence,
        )

    # Convert to list of tuples for replay
    events_to_replay = [(event.event_id, event.data) for event in cached_events]

    logger.info(
        "SSE reconnection: resuming task %s from sequence %d, replaying %d events",
        task_id,
        sequence,
        len(events_to_replay),
    )

    return SSEReconnectResult(
        status=SSEReconnectStatus.RESUMED,
        cached_events=events_to_replay,
        resume_sequence=sequence,
    )


def create_reconnect_generator(
    task_id: str,
    last_event_id: str | None,
    original_generator: Generator[str, None, None] | None = None,
) -> Generator[str, None, None]:
    """
    Create a generator that replays cached events and optionally continues with live events.

    This is used when a client reconnects after a connection drop.
    It first yields any cached events that the client missed, then optionally
    continues with the original generator for live events.

    Args:
        task_id: The task ID for the SSE stream.
        last_event_id: The Last-Event-ID header value from the client.
        original_generator: Optional generator for live events to continue with.

    Yields:
        SSE formatted event strings.
    """
    result = try_reconnect(task_id, last_event_id)

    # First, replay any cached events
    if result.has_cached_events:
        for event_id, data in result.cached_events:
            # The data is already formatted as SSE, just yield it
            yield data

    # If we have an original generator, continue with live events
    if original_generator is not None:
        yield from original_generator


def wrap_generator_with_cache(
    task_id: str,
    generator: Generator[Any, None, None],
    format_event: Callable[[Any], str] | None = None,
) -> Generator[str, None, None]:
    """
    Wrap a generator to cache events for potential reconnection.

    This wrapper intercepts events from the original generator, caches them,
    and adds event IDs for SSE reconnection support.

    Args:
        task_id: The task ID for the SSE stream.
        generator: The original event generator.
        format_event: Optional function to format events as SSE strings.

    Yields:
        SSE formatted event strings with event IDs.
    """
    cache = SSEEventCache(task_id)

    for event in generator:
        # Format the event if a formatter is provided
        if format_event is not None:
            formatted_event = format_event(event)
        else:
            formatted_event = str(event)

        if cache.enabled:
            # Generate event ID and cache the event
            event_id = cache.generate_event_id()

            # Add event ID to the SSE output
            # SSE format: id: {event_id}\n{data}
            sse_with_id = f"id: {event_id}\n{formatted_event}"

            # Cache the complete SSE event (with id)
            cache.push_event(event_id, sse_with_id)

            yield sse_with_id
        else:
            # SSE reconnection disabled, just yield the formatted event
            yield formatted_event
