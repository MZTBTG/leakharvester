import pytest
from unittest.mock import MagicMock, patch
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.domain.exceptions import EnvironmentMismatchError

def test_safe_fail_on_id_mismatch():
    """
    Verifies that ClickHouseAdapter raises EnvironmentMismatchError
    when the local configured ID does not match the server-side ID.
    """
    with patch("clickhouse_connect.get_client") as mock_get_client, \
         patch("leakharvester.adapters.clickhouse.settings") as mock_settings:
        
        # Setup Client Mock
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance
        
        # Scenario: Mismatch
        # Local ID: "local-uuid"
        # Server ID: "server-uuid"
        
        mock_settings.INSTANCE_ID = "local-uuid"
        
        # Mock query response for server ID
        # Expected query: SELECT value FROM vault.system_info WHERE key = 'instance_id'
        mock_result = MagicMock()
        mock_result.result_rows = [("server-uuid",)]
        mock_client_instance.query.return_value = mock_result
        
        adapter = ClickHouseAdapter()
        
        # Accessing .client should trigger the check and raise Error
        with pytest.raises(EnvironmentMismatchError):
            _ = adapter.client

def test_success_on_id_match():
    """
    Verifies that ClickHouseAdapter succeeds when IDs match.
    """
    with patch("clickhouse_connect.get_client") as mock_get_client, \
         patch("leakharvester.adapters.clickhouse.settings") as mock_settings:
        
        # Setup Client Mock
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance
        
        mock_settings.INSTANCE_ID = "match-uuid"
        
        # Mock query response for server ID
        mock_result = MagicMock()
        mock_result.result_rows = [("match-uuid",)]
        mock_client_instance.query.return_value = mock_result
        
        adapter = ClickHouseAdapter()
        
        # Should not raise
        client = adapter.client
        assert client == mock_client_instance

def test_skip_check_if_no_local_id():
    """
    Verifies that if local settings have no ID (legacy/fresh install), 
    we skip the check (or handle it gracefully).
    """
    with patch("clickhouse_connect.get_client") as mock_get_client, \
         patch("leakharvester.adapters.clickhouse.settings") as mock_settings:
        
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance
        
        mock_settings.INSTANCE_ID = None
        
        adapter = ClickHouseAdapter()
        
        # Should not raise
        _ = adapter.client
        # Should not have queried system_info
        mock_client_instance.query.assert_not_called()