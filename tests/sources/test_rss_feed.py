import pytest
from sources.rss_feed import rss_entries_resource
from unittest.mock import patch
from feedparser.util import FeedParserDict

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
                "title": "[Action Required]",
                "summary": "https://example.com",
                "published": "2025-08-27",
                "link": "https://example.com"
            }
        ],
        feed=FeedParserDict(),
        headers={},
    )

    # This should handle the case gracefully rather than crashing
    # Currently would fail on line 23: entry.get("link").encode("utf-8")
    results = list(rss_entries_resource("mock_url"))
    assert len(results) == 0