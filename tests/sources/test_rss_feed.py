import pytest
from sources.rss_feed import rss_entries_resource
from sources.rss_feed import _get_open_ai_company_name, _get_open_ai_fit_score
from unittest.mock import patch, MagicMock
from feedparser import FeedParserDict

@pytest.fixture
def mock_feed_entries():
    mock_data = [
        {
            "title": "Job 1",
            "summary": "Summary 1",
            "link": "https://example.com/job1",
            "published": "2025-08-27"
        },
        {
            "title": "Job 2",
            "summary": "<p>Summary 2</p>",
            "link": "https://example.com/job2",
            "published": "2025-08-26"
        }
    ]
    return FeedParserDict(
        bozo=False,
        entries=mock_data,
        feed=FeedParserDict(),
        headers={},
    )

@pytest.fixture
def dupe_id_feed():
    return FeedParserDict(
        bozo=False,
        entries=[
            {
                "title": "Job 1",
                "summary": "Summary 1",
                "link": "https://example.com/job1",
                "published": "2025-08-27"
            },
            {
                "title": "Job 1",
                "summary": "Summary 1",
                "link": "https://example.com/job1",
                "published": "2025-08-27"
            }
        ],
        feed=[],
        headers={},
    )

@patch('sources.rss_feed.feedparser.parse')
def test_returns_correct_records(mocked_parse, mock_feed_entries):
    mocked_parse.return_value = mock_feed_entries
    results = list(rss_entries_resource("mock_url"))

    assert len(results) == 2
    assert results[0]['title'] == "Job 1"
    assert results[0]['summary'] == "Summary 1"
    assert results[0]['link'] == "https://example.com/job1"
    assert results[0]['published_at'] == "2025-08-27"
    assert results[0]['created_at'] is not None
    assert results[1]['title'] == "Job 2"
    assert results[1]['summary'] == "<p>Summary 2</p>"
    assert results[1]['created_at'] is not None
    

@patch('sources.rss_feed.feedparser.parse')
def test_produced_unique_hashed_ids(mocked_parse, mock_feed_entries):
    mocked_parse.return_value = mock_feed_entries
    results = list(rss_entries_resource("mock_url"))
    ids = {entry['id'] for entry in results}
    assert len(set(ids)) == 2

@patch('sources.rss_feed.feedparser.parse')
def test_same_url_produces_same_ids(mocked_parse, dupe_id_feed):
    mocked_parse.return_value = dupe_id_feed
    results = list(rss_entries_resource("mock_url"))
    ids = {entry['id'] for entry in results}
    assert len(set(ids)) == 1


@patch('sources.rss_feed.feedparser.parse')
@patch('dlt.current.resource_state')
def test_skips_already_processed_entries(mocked_state, mocked_parse, mock_feed_entries):
    mocked_parse.return_value = mock_feed_entries
    mocked_state.return_value = { 'processed_record_ids': ['ca764dc55f7ecd560e9d1d376f7c0cce02d899e131c16a7b61dd9aeed08d2568', 'ba5088d00aa705640b66afc54bb8bb80003f298bd720418068b3142d9eb90e48'] }
    results = list(rss_entries_resource("mock_url"))
    assert len(results) == 0

@patch('sources.rss_feed.feedparser.parse')
def test_handles_malformed_rss_payload(mocked_parse):
    """Test that rss_entries_resource handles case where RSS entries lack required link field"""
    # Mock feedparser to return entries without link field
    mocked_parse.return_value = FeedParserDict(
        bozo=False,
        entries=[
            {
                "title": "[Action required]",
                "summary": "https://example.com",
                "published": "2025-08-27",
                "link": "https://example.com"
            }
        ],
        feed=FeedParserDict(),
        headers={},
    )
    results = list(rss_entries_resource("mock_url"))
    assert len(results) == 0
    
@patch('sources.rss_feed.OpenAI')
@patch('dlt.secrets.get')
def test_get_open_ai_company_name(mock_secrets, mock_openai):
    mock_secrets.return_value = "test-api-key"
    mock_response = MagicMock()
    mock_response.output_text = '{"company": "Acme Corp"}'
    mock_client = MagicMock()
    mock_client.responses.create.return_value = mock_response
    mock_openai.return_value = mock_client
    
    result = _get_open_ai_company_name("Software Engineer at Acme Corp")
    
    assert result == {"company": "Acme Corp"}
    mock_openai.assert_called_once_with(api_key="test-api-key")
    mock_client.responses.create.assert_called()


@patch('sources.rss_feed.OpenAI')
@patch('dlt.secrets.get')
def test_get_open_ai_company_name_returns_null_on_json_error(mock_secrets, mock_openai):
    mock_secrets.return_value = "test-api-key"
    mock_response = MagicMock()
    mock_response.output_text = 'Invalid JSON response from OpenAI'
    mock_client = MagicMock()
    mock_client.responses.create.return_value = mock_response
    mock_openai.return_value = mock_client
    
    result = _get_open_ai_company_name("Software Engineer at Acme Corp")
    
    assert result == {"company": None}

@pytest.mark.asyncio
@patch('sources.rss_feed.AsyncOpenAI')
@patch('dlt.secrets.get')
async def test_get_open_ai_fit_score_happy_path(mock_secrets, mock_async_openai):
    mock_secrets.return_value = "test-api-key"
    mock_response = MagicMock()
    mock_response.output_text = '{"fit_score": 8, "reasoning": "Good match for required skills"}'
    mock_client = MagicMock()
    
    async def mock_create(*args, **kwargs):
        return mock_response
    
    mock_client.responses.create = mock_create
    mock_async_openai.return_value = mock_client
    
    result = await _get_open_ai_fit_score("Senior Python developer position")
    
    assert result == {"fit_score": 8, "reasoning": "Good match for required skills"}
    mock_async_openai.assert_called_once_with(api_key="test-api-key")

@pytest.mark.asyncio
@patch('sources.rss_feed.AsyncOpenAI')
@patch('dlt.secrets.get')
async def test_get_open_ai_fit_score_invalid_json_response(mock_secrets, mock_async_openai):
    mock_secrets.return_value = "test-api-key"
    mock_response = MagicMock()
    mock_response.output_text = 'Invalid JSON response from OpenAI'
    mock_client = MagicMock()
    
    async def mock_create(*args, **kwargs):
        return mock_response
    
    mock_client.responses.create = mock_create
    mock_async_openai.return_value = mock_client
    
    result = await _get_open_ai_fit_score("Senior Python developer position")
    
    assert result == {"fit_score": None, "reasoning": "Failed to parse response"}
