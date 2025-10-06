import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from preprocess import (
    extract_customer_info,
    generate_embedding,
    batch_fetch_customers
)


def test_extract_customer_info_with_data():
    """Test customer info extraction with complete data."""
    customer = {
        "fullName": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "gender": "male"
    }
    
    result = extract_customer_info(customer)
    
    assert result["fullName"] == "John Doe"
    assert result["email"] == "john@example.com"
    assert result["age"] == 30


def test_extract_customer_info_empty():
    """Test customer info extraction with no data."""
    result = extract_customer_info(None)
    assert result == {}


@patch('preprocess.openai.embeddings.create')
def test_generate_embedding_success(mock_create):
    """Test successful embedding generation."""
    mock_response = Mock()
    mock_response.data = [Mock(embedding=[0.1, 0.2, 0.3])]
    mock_create.return_value = mock_response
    
    result = generate_embedding("test text")
    
    assert result == [0.1, 0.2, 0.3]
    mock_create.assert_called_once()


@patch('preprocess.openai.embeddings.create')
def test_generate_embedding_failure(mock_create):
    """Test embedding generation with error."""
    mock_create.side_effect = Exception("API Error")
    
    result = generate_embedding("test text")
    
    assert result is None


def test_batch_fetch_customers():
    """Test batch customer fetching."""
    # This would require mocking MongoDB
    # Placeholder for demonstration
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])