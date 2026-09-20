"""Unit tests for log search endpoint.

Tests search query building, filtering logic, and pagination.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from src.models.log_entry import SearchRequest, LogSearchResponse


class TestSearchRequest:
    """Tests for SearchRequest validation and defaults."""
    
    def test_default_time_range(self):
        """Test that default time range is set to last 24 hours."""
        search = SearchRequest()
        
        assert search.time_from is not None
        assert search.time_to is not None
        
        # time_to should be approximately now
        now = datetime.utcnow()
        time_diff = abs((search.time_to - now).total_seconds())
        assert time_diff < 5  # Within 5 seconds
        
        # time_from should be 24 hours before time_to
        expected_time_from = search.time_to - timedelta(hours=24)
        time_diff = abs((search.time_from - expected_time_from).total_seconds())
        assert time_diff < 5
    
    def test_custom_time_range(self):
        """Test custom time range overrides defaults."""
        time_from = datetime(2024, 1, 15, 10, 0, 0)
        time_to = datetime(2024, 1, 16, 10, 0, 0)
        
        search = SearchRequest(
            time_from=time_from,
            time_to=time_to
        )
        
        assert search.time_from == time_from
        assert search.time_to == time_to
    
    def test_level_normalization(self):
        """Test that log level is normalized to uppercase."""
        search = SearchRequest(level="error")
        assert search.level == "ERROR"
        
        search = SearchRequest(level="warning")
        assert search.level == "WARNING"
    
    def test_pagination_defaults(self):
        """Test default pagination values."""
        search = SearchRequest()
        assert search.offset == 0
        assert search.limit == 100
    
    def test_pagination_validation(self):
        """Test pagination bounds validation."""
        # Valid pagination
        search = SearchRequest(offset=0, limit=1)
        assert search.offset == 0
        assert search.limit == 1
        
        search = SearchRequest(offset=1000, limit=1000)
        assert search.offset == 1000
        assert search.limit == 1000
        
        # Invalid negative offset
        with pytest.raises(Exception):  # Pydantic ValidationError
            SearchRequest(offset=-1)
        
        # Invalid limit (too small)
        with pytest.raises(Exception):
            SearchRequest(limit=0)
        
        # Invalid limit (too large)
        with pytest.raises(Exception):
            SearchRequest(limit=1001)
    
    def test_all_filters(self):
        """Test search with all filters applied."""
        search = SearchRequest(
            source="http",
            source_service="auth-api",
            level="ERROR",
            time_from=datetime(2024, 1, 15, 10, 0, 0),
            time_to=datetime(2024, 1, 16, 10, 0, 0),
            query="database connection",
            trace_id="550e8400-e29b-41d4-a716-446655440000",
            user_id="user123",
            offset=50,
            limit=200,
        )
        
        assert search.source == "http"
        assert search.source_service == "auth-api"
        assert search.level == "ERROR"
        assert search.query == "database connection"
        assert search.trace_id == "550e8400-e29b-41d4-a716-446655440000"
        assert search.user_id == "user123"
        assert search.offset == 50
        assert search.limit == 200


class TestLogSearchResponse:
    """Tests for LogSearchResponse model."""
    
    def test_valid_response(self):
        """Test creating valid search response."""
        response = LogSearchResponse(
            total=1500,
            offset=0,
            limit=100,
            items=[
                {
                    "id": "550e8400-e29b-41d4-a716-446655440000",
                    "created_at": "2024-01-15T10:30:00Z",
                    "source": "http",
                    "source_service": "auth-api",
                    "level": "ERROR",
                    "message": "Test error",
                }
            ],
            query_time_ms=45,
        )
        
        assert response.total == 1500
        assert response.offset == 0
        assert response.limit == 100
        assert len(response.items) == 1
        assert response.query_time_ms == 45
    
    def test_empty_results(self):
        """Test response with no results."""
        response = LogSearchResponse(
            total=0,
            offset=0,
            limit=100,
            items=[],
            query_time_ms=10,
        )
        
        assert response.total == 0
        assert len(response.items) == 0


class TestClickHouseSearchQuery:
    """Tests for ClickHouse search_logs query building."""
    
    @pytest.fixture
    def mock_clickhouse_client(self):
        """Mock ClickHouse client."""
        with patch('src.clickhouse.client.clickhouse_connect.get_client') as mock_get_client:
            mock_client = MagicMock()
            
            # Mock query result
            mock_result = MagicMock()
            mock_result.result_rows = [[100]]  # Count query
            mock_result.column_names = [
                'id', 'created_at', 'source', 'source_host', 'source_service',
                'level', 'message', 'parsed_message', 'format', 'metadata',
                'user_id', 'request_id', 'trace_id', 'span_id',
                'http_method', 'http_path', 'http_status', 'http_response_time_ms',
                'error_type', 'error_stack', 'is_parsed', 'parse_errors'
            ]
            
            mock_client.query.return_value = mock_result
            mock_get_client.return_value = mock_client
            
            from src.clickhouse.client import ClickHouseClient
            client = ClickHouseClient(
                host="test_host",
                port=9000,
                database="test_db",
                user="test_user",
                password="test_pass"
            )
            client.connect()
            
            yield client, mock_client
    
    def test_search_with_source_filter(self, mock_clickhouse_client):
        """Test search query with source filter."""
        client, mock_client = mock_clickhouse_client
        
        results, total = client.search_logs(
            source="http",
            limit=10
        )
        
        # Check that query was called
        assert mock_client.query.call_count == 2  # Count + data query
        
        # Check parameters
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        assert 'source' in params
        assert params['source'] == "http"
    
    def test_search_with_level_filter(self, mock_clickhouse_client):
        """Test search query with level filter."""
        client, mock_client = mock_clickhouse_client
        
        results, total = client.search_logs(
            level="ERROR",
            limit=10
        )
        
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        assert 'level' in params
        assert params['level'] == "ERROR"
    
    def test_search_with_time_range(self, mock_clickhouse_client):
        """Test search query with time range filter."""
        client, mock_client = mock_clickhouse_client
        
        time_from = "2024-01-15T00:00:00"
        time_to = "2024-01-16T00:00:00"
        
        results, total = client.search_logs(
            time_from=time_from,
            time_to=time_to,
            limit=10
        )
        
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        assert 'time_from' in params
        assert 'time_to' in params
        assert params['time_from'] == time_from
        assert params['time_to'] == time_to
    
    def test_search_with_full_text_query(self, mock_clickhouse_client):
        """Test search query with full-text search."""
        client, mock_client = mock_clickhouse_client
        
        results, total = client.search_logs(
            query="database connection",
            limit=10
        )
        
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        assert 'query_pattern' in params
        # Check that query is wrapped with % for ILIKE
        assert params['query_pattern'] == "%database connection%"
    
    def test_search_with_trace_id(self, mock_clickhouse_client):
        """Test search query with trace_id filter."""
        client, mock_client = mock_clickhouse_client
        
        trace_id = "550e8400-e29b-41d4-a716-446655440000"
        results, total = client.search_logs(
            trace_id=trace_id,
            limit=10
        )
        
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        assert 'trace_id' in params
        assert params['trace_id'] == trace_id
    
    def test_search_with_pagination(self, mock_clickhouse_client):
        """Test search query with pagination."""
        client, mock_client = mock_clickhouse_client
        
        results, total = client.search_logs(
            offset=50,
            limit=200
        )
        
        # Check data query (second call)
        call_args = mock_client.query.call_args_list[1]
        params = call_args.kwargs.get('parameters', {})
        assert params['offset'] == 50
        assert params['limit'] == 200
    
    def test_search_sql_injection_prevention(self, mock_clickhouse_client):
        """Test that parameterized queries prevent SQL injection."""
        client, mock_client = mock_clickhouse_client
        
        # Attempt SQL injection in query parameter
        malicious_query = "'; DROP TABLE logs; --"
        
        results, total = client.search_logs(
            query=malicious_query,
            limit=10
        )
        
        # Check that query is passed as parameter, not injected into SQL
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        # Parameter should contain the malicious string safely wrapped
        assert 'query_pattern' in params
        assert params['query_pattern'] == f"%{malicious_query}%"
        
        # Check that SQL string itself doesn't contain the injection
        sql = call_args.args[0] if call_args.args else ""
        # SQL should use parameter placeholder, not direct string injection
        assert "DROP TABLE" not in sql
        assert "%(query_pattern)s" in sql
    
    def test_search_returns_tuple(self, mock_clickhouse_client):
        """Test that search_logs returns (results, total) tuple."""
        client, mock_client = mock_clickhouse_client
        
        result = client.search_logs(limit=10)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        
        results, total = result
        assert isinstance(results, list)
        assert isinstance(total, int)
    
    def test_search_with_all_filters(self, mock_clickhouse_client):
        """Test search with all filters combined."""
        client, mock_client = mock_clickhouse_client
        
        results, total = client.search_logs(
            source="http",
            source_service="auth-api",
            level="ERROR",
            time_from="2024-01-15T00:00:00",
            time_to="2024-01-16T00:00:00",
            query="database",
            trace_id="550e8400-e29b-41d4-a716-446655440000",
            user_id="user123",
            offset=0,
            limit=100
        )
        
        # Verify all parameters are passed
        call_args = mock_client.query.call_args_list[0]
        params = call_args.kwargs.get('parameters', {})
        
        assert 'source' in params
        assert 'source_service' in params
        assert 'level' in params
        assert 'time_from' in params
        assert 'time_to' in params
        assert 'query_pattern' in params
        assert 'trace_id' in params
        assert 'user_id' in params


class TestSearchPerformance:
    """Tests for search performance optimizations."""
    
    @pytest.fixture
    def mock_clickhouse_client(self):
        """Mock ClickHouse client."""
        with patch('src.clickhouse.client.clickhouse_connect.get_client') as mock_get_client:
            mock_client = MagicMock()
            
            # Mock query result
            mock_result = MagicMock()
            mock_result.result_rows = [[100]]  # Count query
            mock_result.column_names = [
                'id', 'created_at', 'source', 'source_host', 'source_service',
                'level', 'message', 'parsed_message', 'format', 'metadata',
                'user_id', 'request_id', 'trace_id', 'span_id',
                'http_method', 'http_path', 'http_status', 'http_response_time_ms',
                'error_type', 'error_stack', 'is_parsed', 'parse_errors'
            ]
            
            mock_client.query.return_value = mock_result
            mock_get_client.return_value = mock_client
            
            from src.clickhouse.client import ClickHouseClient
            client = ClickHouseClient(
                host="test_host",
                port=9000,
                database="test_db",
                user="test_user",
                password="test_pass"
            )
            client.connect()
            
            yield client, mock_client
    
    def test_default_time_range_for_partition_pruning(self):
        """Test that default time range enables partition pruning."""
        search = SearchRequest()
        
        # Default time range should be set (last 24h)
        assert search.time_from is not None
        assert search.time_to is not None
        
        # Time range should be reasonable for partition pruning
        time_range = (search.time_to - search.time_from).total_seconds()
        expected_range = 24 * 3600  # 24 hours in seconds
        
        # Allow small tolerance for test execution time
        assert abs(time_range - expected_range) < 10
    
    def test_query_includes_order_by(self, mock_clickhouse_client):
        """Test that query includes ORDER BY for consistent results."""
        client, mock_client = mock_clickhouse_client
        
        client.search_logs(limit=10)
        
        # Check data query (second call)
        call_args = mock_client.query.call_args_list[1]
        sql = call_args.args[0] if call_args.args else ""
        
        # Should order by created_at DESC (most recent first)
        assert "ORDER BY created_at DESC" in sql

