"""
Unit tests for SSE Event Cache service.
"""

from unittest.mock import patch

import pytest

from core.app.sse.event_cache import (
    CachedSSEEvent,
    SSEEventCache,
    is_sse_reconnect_enabled,
    parse_last_event_id,
)


class TestParseLastEventId:
    """Tests for parse_last_event_id function."""

    def test_parse_valid_event_id(self):
        """Test parsing a valid last event id."""
        task_id, sequence = parse_last_event_id("task-123:42")
        assert task_id == "task-123"
        assert sequence == 42

    def test_parse_uuid_task_id(self):
        """Test parsing event id with UUID task id."""
        task_id, sequence = parse_last_event_id("550e8400-e29b-41d4-a716-446655440000:100")
        assert task_id == "550e8400-e29b-41d4-a716-446655440000"
        assert sequence == 100

    def test_parse_none_input(self):
        """Test parsing None input."""
        task_id, sequence = parse_last_event_id(None)
        assert task_id is None
        assert sequence is None

    def test_parse_empty_string(self):
        """Test parsing empty string."""
        task_id, sequence = parse_last_event_id("")
        assert task_id is None
        assert sequence is None

    def test_parse_invalid_format_no_colon(self):
        """Test parsing invalid format without colon."""
        task_id, sequence = parse_last_event_id("task123-42")
        assert task_id is None
        assert sequence is None

    def test_parse_invalid_sequence(self):
        """Test parsing event id with non-numeric sequence."""
        task_id, sequence = parse_last_event_id("task-123:abc")
        assert task_id is None
        assert sequence is None

    def test_parse_multiple_colons(self):
        """Test parsing event id with multiple colons (takes last part as sequence)."""
        task_id, sequence = parse_last_event_id("task:with:colons:42")
        assert task_id == "task:with:colons"
        assert sequence == 42


class TestSSEEventCache:
    """Tests for SSEEventCache class."""

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client."""
        with patch("core.app.sse.event_cache.redis_client") as mock:
            yield mock

    @pytest.fixture
    def mock_config_enabled(self):
        """Mock configuration with SSE reconnect enabled."""
        with patch("core.app.sse.event_cache.dify_config") as mock:
            mock.SSE_RECONNECT_ENABLED = True
            mock.SSE_EVENT_CACHE_TTL = 600
            mock.SSE_EVENT_CACHE_MAX_SIZE = 1000
            yield mock

    @pytest.fixture
    def mock_config_disabled(self):
        """Mock configuration with SSE reconnect disabled."""
        with patch("core.app.sse.event_cache.dify_config") as mock:
            mock.SSE_RECONNECT_ENABLED = False
            mock.SSE_EVENT_CACHE_TTL = 600
            mock.SSE_EVENT_CACHE_MAX_SIZE = 1000
            yield mock

    def test_cache_enabled_property(self, mock_redis, mock_config_enabled):
        """Test enabled property returns correct value."""
        cache = SSEEventCache("task-123")
        assert cache.enabled is True

    def test_cache_disabled_property(self, mock_redis, mock_config_disabled):
        """Test enabled property when disabled."""
        cache = SSEEventCache("task-123")
        assert cache.enabled is False

    def test_task_id_property(self, mock_redis, mock_config_enabled):
        """Test task_id property."""
        cache = SSEEventCache("my-task-id")
        assert cache.task_id == "my-task-id"

    def test_generate_event_id(self, mock_redis, mock_config_enabled):
        """Test event ID generation."""
        cache = SSEEventCache("task-123")
        
        event_id_1 = cache.generate_event_id()
        event_id_2 = cache.generate_event_id()
        event_id_3 = cache.generate_event_id()
        
        assert event_id_1 == "task-123:1"
        assert event_id_2 == "task-123:2"
        assert event_id_3 == "task-123:3"

    def test_push_event_enabled(self, mock_redis, mock_config_enabled):
        """Test pushing an event when enabled."""
        mock_redis.llen.return_value = 5
        
        cache = SSEEventCache("task-123")
        result = cache.push_event("task-123:1", "data: test\n\n")
        
        assert result is True
        mock_redis.rpush.assert_called_once()
        mock_redis.expire.assert_called_once()

    def test_push_event_disabled(self, mock_redis, mock_config_disabled):
        """Test pushing an event when disabled."""
        cache = SSEEventCache("task-123")
        result = cache.push_event("task-123:1", "data: test\n\n")
        
        assert result is False
        mock_redis.rpush.assert_not_called()

    def test_push_event_trims_when_exceeds_max(self, mock_redis, mock_config_enabled):
        """Test that list is trimmed when exceeding max size."""
        mock_redis.llen.return_value = 1001  # Exceeds max of 1000
        
        cache = SSEEventCache("task-123")
        cache.push_event("task-123:1", "data: test\n\n")
        
        mock_redis.ltrim.assert_called_once()

    def test_get_events_after_disabled(self, mock_redis, mock_config_disabled):
        """Test get_events_after when disabled."""
        cache = SSEEventCache("task-123")
        events = cache.get_events_after("task-123:5")
        
        assert events == []
        mock_redis.lrange.assert_not_called()

    def test_get_events_after_invalid_format(self, mock_redis, mock_config_enabled):
        """Test get_events_after with invalid last_event_id format."""
        cache = SSEEventCache("task-123")
        events = cache.get_events_after("invalid-format")
        
        assert events == []

    def test_get_events_after_task_mismatch(self, mock_redis, mock_config_enabled):
        """Test get_events_after with mismatched task ID."""
        cache = SSEEventCache("task-123")
        events = cache.get_events_after("different-task:5")
        
        assert events == []

    def test_get_events_after_success(self, mock_redis, mock_config_enabled):
        """Test get_events_after successfully retrieves events."""
        # Mock cached events
        mock_redis.lrange.return_value = [
            b"task-123:1\x00data: event1\n\n",
            b"task-123:2\x00data: event2\n\n",
            b"task-123:3\x00data: event3\n\n",
            b"task-123:4\x00data: event4\n\n",
        ]
        
        cache = SSEEventCache("task-123")
        events = cache.get_events_after("task-123:2")
        
        assert len(events) == 2
        assert events[0] == CachedSSEEvent(event_id="task-123:3", data="data: event3\n\n")
        assert events[1] == CachedSSEEvent(event_id="task-123:4", data="data: event4\n\n")

    def test_get_events_after_no_new_events(self, mock_redis, mock_config_enabled):
        """Test get_events_after when no events after given sequence."""
        mock_redis.lrange.return_value = [
            b"task-123:1\x00data: event1\n\n",
            b"task-123:2\x00data: event2\n\n",
        ]
        
        cache = SSEEventCache("task-123")
        events = cache.get_events_after("task-123:2")
        
        assert events == []

    def test_get_all_events(self, mock_redis, mock_config_enabled):
        """Test get_all_events retrieves all cached events."""
        mock_redis.lrange.return_value = [
            b"task-123:1\x00data: event1\n\n",
            b"task-123:2\x00data: event2\n\n",
        ]
        
        cache = SSEEventCache("task-123")
        events = cache.get_all_events()
        
        assert len(events) == 2
        assert events[0] == CachedSSEEvent(event_id="task-123:1", data="data: event1\n\n")
        assert events[1] == CachedSSEEvent(event_id="task-123:2", data="data: event2\n\n")

    def test_clear(self, mock_redis, mock_config_enabled):
        """Test clearing the cache."""
        cache = SSEEventCache("task-123")
        result = cache.clear()
        
        assert result is True
        mock_redis.delete.assert_called_once()

    def test_clear_disabled(self, mock_redis, mock_config_disabled):
        """Test clearing when disabled."""
        cache = SSEEventCache("task-123")
        result = cache.clear()
        
        assert result is False
        mock_redis.delete.assert_not_called()

    def test_exists(self, mock_redis, mock_config_enabled):
        """Test exists check."""
        mock_redis.exists.return_value = 1
        
        cache = SSEEventCache("task-123")
        result = cache.exists()
        
        assert result is True

    def test_exists_not_found(self, mock_redis, mock_config_enabled):
        """Test exists when cache doesn't exist."""
        mock_redis.exists.return_value = 0
        
        cache = SSEEventCache("task-123")
        result = cache.exists()
        
        assert result is False

    def test_get_event_count(self, mock_redis, mock_config_enabled):
        """Test getting event count."""
        mock_redis.llen.return_value = 42
        
        cache = SSEEventCache("task-123")
        count = cache.get_event_count()
        
        assert count == 42

    def test_redis_error_handling(self, mock_redis, mock_config_enabled):
        """Test graceful handling of Redis errors."""
        mock_redis.rpush.side_effect = Exception("Redis connection error")
        
        cache = SSEEventCache("task-123")
        result = cache.push_event("task-123:1", "data: test\n\n")
        
        assert result is False  # Should return False, not raise


class TestIsSSEReconnectEnabled:
    """Tests for is_sse_reconnect_enabled function."""

    def test_enabled(self):
        """Test when SSE reconnect is enabled."""
        with patch("core.app.sse.event_cache.dify_config") as mock:
            mock.SSE_RECONNECT_ENABLED = True
            assert is_sse_reconnect_enabled() is True

    def test_disabled(self):
        """Test when SSE reconnect is disabled."""
        with patch("core.app.sse.event_cache.dify_config") as mock:
            mock.SSE_RECONNECT_ENABLED = False
            assert is_sse_reconnect_enabled() is False
