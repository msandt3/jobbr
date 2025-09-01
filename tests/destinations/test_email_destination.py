from unittest.mock import Mock, patch
from destinations.email_destination import _generate_email_body, _send_email


def test_email_body_formatting():
    items = [
        {
            "title": "Software Engineer",
            "company_name": "Tech Corp",
            "fit_score": 85,
            "reasoning": "Great match based on skills and experience.",
            "link": "https://example.com/job1"
        },
        {
            "title": "Data Scientist",
            "company_name": "Data Inc",
            "fit_score": 90,
            "reasoning": "Excellent fit for data analysis role.",
            "link": "https://example.com/job2"
        }
    ]

    email_body = "Here are the top job recommendations:\n\n"
    for item in items:
        email_body += f"""
            Title: {item.get("title", "Unknown")}
            Company: {item.get("company_name", "Unknown")}
            Fit Score: {item.get("fit_score" , "Unknown")}
            Reasoning: {item.get("reasoning", "Unknown")}
            Link: {item.get("link", "Unknown")}
            \n\n\n
        """
    body = _generate_email_body(items)
    assert body == email_body

def test_email_body_with_missing_fields():
    items = [
        {
            "title": "Software Engineer",
            "fit_score": 85,
            "reasoning": "Great match based on skills and experience.",
            "link": "https://example.com/job1"
        }
    ]

    want = f"""Here are the top job recommendations:\n\n
            Title: Software Engineer
            Company: Unknown
            Fit Score: 85
            Reasoning: Great match based on skills and experience.
            Link: https://example.com/job1
            \n\n\n
        """
    got = _generate_email_body(items)
    assert want == got

def test_email_body_with_no_items():
    body = _generate_email_body([])
    assert body == "Here are the top job recommendations:\n\n"    

@patch('requests.post')
def test_send_email(patched_requests):
    # This is a placeholder test to ensure the email sending function can be called
    # In a real test, you would mock requests.post and verify it was called with expected parameters
    patched_requests.return_value = Mock(status_code=200, text="OK")
    body = "Test email body"
    _send_email(body)
    assert patched_requests.called

