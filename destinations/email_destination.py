import dlt
from dlt.common.typing import TDataItems
from dlt.common.schema import TTableSchema
import requests
from datetime import datetime

@dlt.destination
def email_destination(items: TDataItems, table: TTableSchema) -> None:
    email_body = "Here are the top job recommendations:\n\n"
    for item in items:
        print(item.keys())
        email_body += f"""
            Title: {item.get("title", "Unknown")}
            Company: {item.get("company_name", "Unknown")}
            Fit Score: {item.get("fit_score" , "Unknown")}
            Reasoning: {item.get("reasoning", "Unknown")}
            Link: {item.get("link", "Unknown")}
            \n\n\n
        """
    _send_email(email_body)
    # Here you would integrate with an email service to send the email_body

def _send_email(body):
    api_key = dlt.secrets.get("mailgun.sending_key")
    sandbox_domain = dlt.secrets.get("mailgun.sandbox_domain")
    base_url = dlt.secrets.get("mailgun.base_url")

    response = requests.post(
        f"{base_url}/v3/{sandbox_domain}/messages",
        auth=("api", api_key),
        data={"from": f"Mailgun Sandbox <postmaster@{sandbox_domain}>",
            "to": ["michael.sandt@gmail.com"],
            "subject": f"Top Job Recommendations for {datetime.now().strftime('%Y-%m-%d')}",
            "text": body
        }
    )
    print(f"Email sent status: {response.status_code}, response: {response.text}")
