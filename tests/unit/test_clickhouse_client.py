"""Unit tests for ClickHouse client.

Tests cover client initialization, insert operations, and error handling.
Uses mocks to avoid requiring actual ClickHouse server.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from clickhouse_connect.driver.exceptions import DatabaseError

from src.clickhouse.client import ClickHouseClient
from src.clickhouse.exceptions import (
    StorageError,
    ConnectionError,
    InsertError,
    QueryError,
)
from src.models.log_entry import LogEntry


class TestClickHouseClient:
    """Test suite for ClickHouseClient."""
    
    def test_client_initialization(self):
        """Test client initialization with config."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        
        assert client.host == "localhost"
        assert client.port == 9000
        assert client.database == "logcelot"
        assert client.user == "test_user"
        assert client.password == "test_pass"
        assert client.client is None
        assert client._connected is False
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_connect_success(self, mock_get_client):
        """Test successful connection to ClickHouse."""
        # Mock client
        mock_client = MagicMock()
        mock_client.command.return_value = 1  # Health check returns 1
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        
        client.connect()
        
        assert client._connected is True
        assert client.client is not None
        mock_get_client.assert_called_once()
        mock_client.command.assert_called_with("SELECT 1")
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_connect_failure(self, mock_get_client):
        """Test connection failure raises ConnectionError."""
        mock_get_client.side_effect = Exception("Connection refused")
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        
        with pytest.raises(ConnectionError) as exc_info:
            client.connect()
        
        assert "Failed to connect" in str(exc_info.value)
        assert client._connected is False
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_check_health_success(self, mock_get_client):
        """Test health check returns True when ClickHouse is healthy."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        assert client.check_health() is True
        mock_client.command.assert_called_with("SELECT 1")
    
    def test_check_health_not_connected(self):
        """Test health check raises error when not connected."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        
        with pytest.raises(QueryError, match="not connected"):
            client.check_health()
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_insert_logs_success(self, mock_get_client):
        """Test successful bulk insert of logs."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_client.insert.return_value = None
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        # Create test logs
        logs = [
            LogEntry(
                source="test",
                source_service="test-service",
                level="info",
                message=f"Test log {i}",
            )
            for i in range(10)
        ]
        
        result = client.insert_logs(logs)
        
        assert result["rows_inserted"] == 10
        assert result["table"] == "logs"
        assert "duration_ms" in result
        
        # Verify insert was called
        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        assert call_args.kwargs["table"] == "logs"
        assert len(call_args.kwargs["data"]) == 10
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_insert_logs_empty_list(self, mock_get_client):
        """Test insert with empty list returns zero rows."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        result = client.insert_logs([])
        
        assert result["rows_inserted"] == 0
        mock_client.insert.assert_not_called()
    
    def test_insert_logs_not_connected(self):
        """Test insert raises error when not connected."""
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        
        logs = [
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
            )
        ]
        
        with pytest.raises(InsertError, match="not connected"):
            client.insert_logs(logs)
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_insert_logs_database_error(self, mock_get_client):
        """Test insert handles database errors."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_client.insert.side_effect = DatabaseError("Table not found")
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        logs = [
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
            )
        ]
        
        with pytest.raises(InsertError) as exc_info:
            client.insert_logs(logs)
        
        assert "Failed to insert" in str(exc_info.value)
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_insert_logs_converts_to_clickhouse_dict(self, mock_get_client):
        """Test that logs are converted using to_clickhouse_dict()."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_client.insert.return_value = None
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        log = LogEntry(
            source="test",
            source_service="test-service",
            level="error",
            message="Test error",
            metadata={"key": "value"},
        )
        
        client.insert_logs([log])
        
        # Verify data was converted correctly
        call_args = mock_client.insert.call_args
        inserted_data = call_args.kwargs["data"]
        column_names = call_args.kwargs["column_names"]
        
        # Data is now list of lists, not list of dicts
        assert len(inserted_data) == 1
        assert isinstance(inserted_data[0], list)
        
        # Create dict from column_names and data for verification
        row_dict = dict(zip(column_names, inserted_data[0]))
        
        assert row_dict["source"] == "test"
        assert row_dict["source_service"] == "test-service"
        assert row_dict["level"] == "ERROR"  # Normalized
        assert row_dict["message"] == "Test error"
        assert '"key": "value"' in row_dict["metadata"]  # JSON serialized
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_query_success(self, mock_get_client):
        """Test successful query execution."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        
        # Mock query result
        mock_result = MagicMock()
        mock_result.column_names = ["id", "message", "level"]
        mock_result.result_rows = [
            ("uuid-1", "Test 1", "INFO"),
            ("uuid-2", "Test 2", "ERROR"),
        ]
        mock_client.query.return_value = mock_result
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        result = client.query("SELECT * FROM logs LIMIT 2")
        
        assert len(result) == 2
        assert result[0]["id"] == "uuid-1"
        assert result[0]["message"] == "Test 1"
        assert result[1]["level"] == "ERROR"
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_graceful_close(self, mock_get_client):
        """Test graceful connection close."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client
        
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
        )
        client.connect()
        
        assert client._connected is True
        
        client.close()
        
        assert client._connected is False
        mock_client.close.assert_called_once()

    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    def test_async_insert_option(self, mock_get_client):
        """Test that async_insert settings are passed to ClickHouse based on client config."""
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_client.insert.return_value = None
        mock_get_client.return_value = mock_client

        logs = [
            LogEntry(
                source="test",
                source_service="test",
                level="info",
                message="Test",
            )
        ]

        # async_insert enabled
        client = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
            async_insert=True,
            wait_for_async_insert=True,
            logs_table="logs",
        )
        client.connect()

        client.insert_logs(logs)

        call_args = mock_client.insert.call_args
        assert call_args.kwargs["settings"]["async_insert"] == 1
        assert call_args.kwargs["settings"]["wait_for_async_insert"] == 1

        # async_insert disabled
        mock_client.insert.reset_mock()

        client2 = ClickHouseClient(
            host="localhost",
            port=9000,
            database="logcelot",
            user="test_user",
            password="test_pass",
            async_insert=False,
            wait_for_async_insert=True,  # irrelevant when async_insert=False
            logs_table="logs",
        )
        client2.client = mock_client
        client2._connected = True

        client2.insert_logs(logs)

        call_args = mock_client.insert.call_args
        assert call_args.kwargs["settings"] == {}


class TestClickHouseClientSingleton:
    """Test suite for singleton pattern."""
    
    @patch('src.clickhouse.client.clickhouse_connect.get_client')
    @patch('src.clickhouse.client.get_settings')
    def test_get_client_singleton(self, mock_get_settings, mock_get_client):
        """Test that get_client returns singleton instance."""
        from src.clickhouse.client import get_client, _client_instance
        
        # Reset singleton
        import src.clickhouse.client
        src.clickhouse.client._client_instance = None
        
        # Mock settings
        mock_settings = MagicMock()
        mock_settings.clickhouse_host = "localhost"
        mock_settings.clickhouse_port = 9000
        mock_settings.clickhouse_database = "logcelot"
        mock_settings.clickhouse_user = "test"
        mock_settings.clickhouse_password = "test"
        mock_get_settings.return_value = mock_settings
        
        # Mock client
        mock_client = MagicMock()
        mock_client.command.return_value = 1
        mock_get_client.return_value = mock_client
        
        # Get client twice
        client1 = get_client()
        client2 = get_client()
        
        # Should be same instance
        assert client1 is client2
        
        # Connection should only be called once
        assert mock_get_client.call_count == 1

