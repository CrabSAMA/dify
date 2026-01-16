"""
Unit tests for SSE reconnection functionality.
"""

from unittest.mock import MagicMock, patch

import pytest

from core.app.sse.event_cache import CachedSSEEvent
from core.app.sse.reconnect import (
    SSEReconnectResult,
    SSEReconnectStatus,
    create_reconnect_generator,
    try_reconnect,
)


class TestSSEReconnectResult:
    """Tests for SSEReconnectResult class."""

    def test_can_resume_true(self):
        """Test can_resume returns True when status is RESUMED and events exist."""
        result = SSEReconnectResult(
            status=SSEReconnectStatus.RESUMED,
            cached_events=[("task:1", "data: test\n\n")],
            resume_sequence=1,
        )
        assert result.can_resume is True

    def test_can_resume_false_no_events(self):
        """Test can_resume returns False when no cached events."""
        result = SSEReconnectResult(
            status=SSEReconnectStatus.RESUMED,
            cached_events=[],
            resume_sequence=1,
        )
        assert result.can_resume is False

    def test_can_resume_false_wrong_status(self):
        """Test can_resume returns False when status is not RESUMED."""
        result = SSEReconnectResult(
            status=SSEReconnectStatus.NO_CACHE,
            cached_events=[("task:1", "data: test\n\n")],
        )
        assert result.can_resume is False

    def test_has_cached_events(self):
        """Test has_cached_events property."""
        result_with_events = SSEReconnectResult(
            status=SSEReconnectStatus.RESUMED,
            cached_events=[("task:1", "data: test\n\n")],
        )
        result_without_events = SSEReconnectResult(
            status=SSEReconnectStatus.RESUMED,
            cached_events=[],
        )
        
        assert result_with_events.has_cached_events is True
        assert result_without_events.has_cached_events is False


class TestTryReconnect:
    """Tests for try_reconnect function."""

    @pytest.fixture
    def mock_config_enabled(self):
        """Mock configuration with SSE reconnect enabled."""
        with patch("core.app.sse.reconnect.SSEEventCache") as MockCache:
            mock_cache = MagicMock()
            mock_cache.enabled = True
            MockCache.return_value = mock_cache
            yield mock_cache

    @pytest.fixture
    def mock_config_disabled(self):
        """Mock configuration with SSE reconnect disabled."""
        with patch("core.app.sse.reconnect.SSEEventCache") as MockCache:
            mock_cache = MagicMock()
            mock_cache.enabled = False
            MockCache.return_value = mock_cache
            yield mock_cache

    def test_reconnect_disabled(self, mock_config_disabled):
        """Test reconnection when feature is disabled."""
        result = try_reconnect("task-123", "task-123:5")
        
        assert result.status == SSEReconnectStatus.DISABLED

    def test_reconnect_no_last_event_id(self, mock_config_enabled):
        """Test reconnection with no last event id."""
        result = try_reconnect("task-123", None)
        
        assert result.status == SSEReconnectStatus.NO_CACHE

    def test_reconnect_invalid_event_id(self, mock_config_enabled):
        """Test reconnection with invalid event id format."""
        with patch("core.app.sse.reconnect.parse_last_event_id") as mock_parse:
            mock_parse.return_value = (None, None)
            
            result = try_reconnect("task-123", "invalid")
            
            assert result.status == SSEReconnectStatus.INVALID_ID

    def test_reconnect_task_mismatch(self, mock_config_enabled):
        """Test reconnection with mismatched task id."""
        with patch("core.app.sse.reconnect.parse_last_event_id") as mock_parse:
            mock_parse.return_value = ("different-task", 5)
            
            result = try_reconnect("task-123", "different-task:5")
            
            assert result.status == SSEReconnectStatus.TASK_MISMATCH

    def test_reconnect_no_cached_events(self, mock_config_enabled):
        """Test reconnection when no cached events exist."""
        mock_config_enabled.get_events_after.return_value = []
        mock_config_enabled.exists.return_value = False
        
        with patch("core.app.sse.reconnect.parse_last_event_id") as mock_parse:
            mock_parse.return_value = ("task-123", 5)
            
            result = try_reconnect("task-123", "task-123:5")
            
            assert result.status == SSEReconnectStatus.NO_CACHE
            assert result.resume_sequence == 5

    def test_reconnect_success(self, mock_config_enabled):
        """Test successful reconnection with cached events."""
        mock_config_enabled.get_events_after.return_value = [
            CachedSSEEvent(event_id="task-123:6", data="data: event6\n\n"),
            CachedSSEEvent(event_id="task-123:7", data="data: event7\n\n"),
        ]
        
        with patch("core.app.sse.reconnect.parse_last_event_id") as mock_parse:
            mock_parse.return_value = ("task-123", 5)
            
            result = try_reconnect("task-123", "task-123:5")
            
            assert result.status == SSEReconnectStatus.RESUMED
            assert len(result.cached_events) == 2
            assert result.cached_events[0] == ("task-123:6", "data: event6\n\n")
            assert result.cached_events[1] == ("task-123:7", "data: event7\n\n")
            assert result.resume_sequence == 5


class TestCreateReconnectGenerator:
    """Tests for create_reconnect_generator function."""

    def test_no_reconnection_needed(self):
        """Test generator without reconnection (no last_event_id)."""
        original_events = ["event1", "event2", "event3"]
        
        def original_gen():
            yield from original_events
        
        with patch("core.app.sse.reconnect.try_reconnect") as mock_try:
            mock_try.return_value = SSEReconnectResult(
                status=SSEReconnectStatus.NO_CACHE,
            )
            
            gen = create_reconnect_generator("task-123", None, original_gen())
            result = list(gen)
            
            assert result == original_events

    def test_reconnection_replays_cached_events(self):
        """Test generator replays cached events before live events."""
        cached_events = [
            ("task-123:1", "data: cached1\n\n"),
            ("task-123:2", "data: cached2\n\n"),
        ]
        original_events = ["live1", "live2"]
        
        def original_gen():
            yield from original_events
        
        with patch("core.app.sse.reconnect.try_reconnect") as mock_try:
            mock_try.return_value = SSEReconnectResult(
                status=SSEReconnectStatus.RESUMED,
                cached_events=cached_events,
                resume_sequence=0,
            )
            
            gen = create_reconnect_generator("task-123", "task-123:0", original_gen())
            result = list(gen)
            
            # Should have cached events followed by live events
            assert result == [
                "data: cached1\n\n",
                "data: cached2\n\n",
                "live1",
                "live2",
            ]

    def test_reconnection_without_original_generator(self):
        """Test generator replays only cached events when no original generator."""
        cached_events = [
            ("task-123:1", "data: cached1\n\n"),
            ("task-123:2", "data: cached2\n\n"),
        ]
        
        with patch("core.app.sse.reconnect.try_reconnect") as mock_try:
            mock_try.return_value = SSEReconnectResult(
                status=SSEReconnectStatus.RESUMED,
                cached_events=cached_events,
                resume_sequence=0,
            )
            
            gen = create_reconnect_generator("task-123", "task-123:0", None)
            result = list(gen)
            
            assert result == [
                "data: cached1\n\n",
                "data: cached2\n\n",
            ]
