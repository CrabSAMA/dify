"""
Unit tests for SSE-related helper functions.
"""

from unittest.mock import patch

import pytest
from flask import Flask

from libs.helper import get_last_event_id


class TestGetLastEventId:
    """Tests for get_last_event_id function."""

    def test_get_last_event_id_present(self):
        """Test getting Last-Event-ID when present in headers."""
        app = Flask(__name__)
        with app.test_request_context(headers={"Last-Event-ID": "task-123:42"}):
            from flask import request
            result = get_last_event_id(request)
            assert result == "task-123:42"

    def test_get_last_event_id_missing(self):
        """Test getting Last-Event-ID when not present."""
        app = Flask(__name__)
        with app.test_request_context():
            from flask import request
            result = get_last_event_id(request)
            assert result is None


class TestCompactGenerateResponseWithSSE:
    """Tests for compact_generate_response with SSE reconnection support."""

    @pytest.fixture
    def flask_app(self):
        """Create a Flask app for testing."""
        app = Flask(__name__)
        return app

    def test_dict_response_returns_json(self, flask_app):
        """Test that dict responses return JSON."""
        from libs.helper import compact_generate_response
        
        with flask_app.app_context():
            response = compact_generate_response({"result": "test"})
            
            assert response.status_code == 200
            assert response.content_type == "application/json; charset=utf-8"

    def test_generator_response_returns_sse(self, flask_app):
        """Test that generator responses return SSE stream."""
        from libs.helper import compact_generate_response
        
        def gen():
            yield "data: test\n\n"
        
        with flask_app.test_request_context():
            response = compact_generate_response(gen())
            
            assert response.status_code == 200
            assert response.mimetype == "text/event-stream"
            assert response.headers.get("Cache-Control") == "no-cache"
            assert response.headers.get("X-Accel-Buffering") == "no"

    def test_sse_response_without_last_event_id(self, flask_app):
        """Test SSE response without last_event_id just yields original events."""
        from libs.helper import compact_generate_response
        
        events = ["event1\n\n", "event2\n\n"]
        
        def gen():
            yield from events
        
        with flask_app.test_request_context():
            response = compact_generate_response(gen())
            result = "".join(response.response)
            
            assert "event1" in result
            assert "event2" in result

    def test_sse_response_with_last_event_id_replays_cached(self, flask_app):
        """Test SSE response with last_event_id replays cached events."""
        from core.app.sse.event_cache import CachedSSEEvent
        from libs.helper import compact_generate_response
        
        original_events = ["live1\n\n", "live2\n\n"]
        cached_events = [
            CachedSSEEvent(event_id="task-123:1", data="cached1\n\n"),
            CachedSSEEvent(event_id="task-123:2", data="cached2\n\n"),
        ]
        
        def gen():
            yield from original_events
        
        with patch("core.app.sse.event_cache.dify_config") as mock_config:
            mock_config.SSE_RECONNECT_ENABLED = True
            mock_config.SSE_EVENT_CACHE_TTL = 600
            mock_config.SSE_EVENT_CACHE_MAX_SIZE = 1000
            
            with patch("core.app.sse.event_cache.redis_client") as mock_redis:
                # Mock Redis to return cached events
                mock_redis.lrange.return_value = [
                    b"task-123:1\x00cached1\n\n",
                    b"task-123:2\x00cached2\n\n",
                ]
                
                with flask_app.test_request_context():
                    response = compact_generate_response(gen(), last_event_id="task-123:0")
                    result = "".join(response.response)
                    
                    # Should contain both cached and live events
                    assert "cached1" in result
                    assert "cached2" in result
                    assert "live1" in result
                    assert "live2" in result

    def test_sse_response_with_invalid_last_event_id(self, flask_app):
        """Test SSE response with invalid last_event_id just yields original events."""
        from libs.helper import compact_generate_response
        
        events = ["event1\n\n"]
        
        def gen():
            yield from events
        
        with patch("core.app.sse.event_cache.dify_config") as mock_config:
            mock_config.SSE_RECONNECT_ENABLED = True
            mock_config.SSE_EVENT_CACHE_TTL = 600
            mock_config.SSE_EVENT_CACHE_MAX_SIZE = 1000
            
            with flask_app.test_request_context():
                # Invalid format - no colon
                response = compact_generate_response(gen(), last_event_id="invalid")
                result = "".join(response.response)
                
                assert "event1" in result

    def test_sse_response_disabled_reconnect(self, flask_app):
        """Test SSE response when reconnection is disabled."""
        from libs.helper import compact_generate_response
        
        events = ["event1\n\n"]
        
        def gen():
            yield from events
        
        with patch("core.app.sse.event_cache.dify_config") as mock_config:
            mock_config.SSE_RECONNECT_ENABLED = False
            mock_config.SSE_EVENT_CACHE_TTL = 600
            mock_config.SSE_EVENT_CACHE_MAX_SIZE = 1000
            
            with flask_app.test_request_context():
                response = compact_generate_response(gen(), last_event_id="task-123:5")
                result = "".join(response.response)
                
                # Should just yield original events without attempting reconnection
                assert "event1" in result
