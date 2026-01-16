"""
Unit tests for SSE event stream conversion with reconnection support in BaseAppGenerator.
"""

from unittest.mock import MagicMock, patch

import pytest

from core.app.apps.base_app_generator import BaseAppGenerator


class TestConvertToEventStreamWithSSE:
    """Tests for convert_to_event_stream with SSE reconnection support."""

    @pytest.fixture
    def mock_cache_enabled(self):
        """Mock SSEEventCache with caching enabled."""
        with patch("core.app.apps.base_app_generator.SSEEventCache") as MockCache:
            mock_cache = MagicMock()
            mock_cache.enabled = True
            mock_cache.generate_event_id.side_effect = lambda: f"task-123:{mock_cache.generate_event_id.call_count}"
            mock_cache.push_event.return_value = True
            MockCache.return_value = mock_cache
            yield mock_cache, MockCache

    @pytest.fixture
    def mock_cache_disabled(self):
        """Mock SSEEventCache with caching disabled."""
        with patch("core.app.apps.base_app_generator.SSEEventCache") as MockCache:
            mock_cache = MagicMock()
            mock_cache.enabled = False
            MockCache.return_value = mock_cache
            yield mock_cache, MockCache

    def test_dict_response_returns_dict(self):
        """Test that dict responses are returned as-is."""
        response = {"result": "test"}
        result = BaseAppGenerator.convert_to_event_stream(response)
        
        assert result == response

    def test_generator_without_task_id_no_cache(self, mock_cache_disabled):
        """Test generator without task_id doesn't use cache."""
        mock_cache, MockCache = mock_cache_disabled
        
        def gen():
            yield {"event": "test", "data": "value"}
            yield "ping"
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen()))
        
        assert len(result) == 2
        assert "data:" in result[0]
        assert "event: ping" in result[1]
        # Cache should not have been used for events
        mock_cache.push_event.assert_not_called()

    def test_generator_with_task_id_uses_cache(self, mock_cache_enabled):
        """Test generator with task_id uses cache."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            yield {"event": "test", "task_id": "task-123"}
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        assert len(result) == 1
        assert "id: task-123:1" in result[0]
        mock_cache.push_event.assert_called_once()

    def test_generator_extracts_task_id_from_first_event(self, mock_cache_enabled):
        """Test that task_id is extracted from the first event if not provided."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            yield {"event": "first", "task_id": "extracted-task-123"}
            yield {"event": "second", "task_id": "extracted-task-123"}
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen()))
        
        # Should initialize cache with extracted task_id
        MockCache.assert_called_once_with("extracted-task-123")
        assert len(result) == 2

    def test_generator_adds_event_id_to_data_events(self, mock_cache_enabled):
        """Test that event IDs are added to data events."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            yield {"event": "message", "content": "Hello"}
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        assert "id: task-123:1\n" in result[0]
        assert "data:" in result[0]

    def test_generator_adds_event_id_to_event_type_events(self, mock_cache_enabled):
        """Test that event IDs are added to event-type events."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            yield "ping"
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        assert "id: task-123:1\n" in result[0]
        assert "event: ping" in result[0]

    def test_generator_caches_events(self, mock_cache_enabled):
        """Test that events are cached for reconnection."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            yield {"event": "message1"}
            yield {"event": "message2"}
            yield "ping"
        
        list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        # Should have called push_event for each event
        assert mock_cache.push_event.call_count == 3

    def test_generator_handles_empty_generator(self, mock_cache_enabled):
        """Test handling of empty generator."""
        mock_cache, MockCache = mock_cache_enabled
        
        def gen():
            return
            yield  # Make it a generator
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        assert result == []

    def test_generator_disabled_cache_no_event_id(self, mock_cache_disabled):
        """Test that no event ID is added when cache is disabled."""
        mock_cache, MockCache = mock_cache_disabled
        
        def gen():
            yield {"event": "test"}
        
        result = list(BaseAppGenerator.convert_to_event_stream(gen(), task_id="task-123"))
        
        assert "id:" not in result[0]
        assert "data:" in result[0]
