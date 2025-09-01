import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
import requests
from datetime import datetime

@dlt.destination
def email_destination(items: TDataItems, table: TTableSchema) -> None:
    _send_email(_generate_email_body(items))

def _send_email(body):
    api_key = dlt.secrets.get("mailgun.sending_key")
    sandbox_domain = dlt.secrets.get("mailgun.sandbox_domain")
    base_url = dlt.secrets.get("mailgun.base_url")

    response = requests.post(
        f"{base_url}/v3/{sandbox_domain}/messages",
        auth=("api", api_key),
        data={"from": f"Mailgun Sandbox <postmaster@{sandbox_domain}>",
            "to": [dlt.secrets.get("mailgun.destination_email")],
            "subject": f"Top Job Recommendations for {datetime.now().strftime('%Y-%m-%d')}",
            "text": body
        }
    )
    print(f"Email sent status: {response.status_code}, response: {response.text}")

def _generate_email_body(items):
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
    return email_body