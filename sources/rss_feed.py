from datetime import datetime
import json
import dlt
import feedparser
import hashlib
from openai import OpenAI, AsyncOpenAI

@dlt.source
def rss_feed_source(rss_feed_url: str):
    return rss_entries_resource(rss_feed_url)

@dlt.resource(table_name="jobs", write_disposition="merge", primary_key="id")
def rss_entries_resource(rss_feed_url: str):
    """Fetch and parse entries from an RSS feed."""
    print("Fetching RSS Feed, yielding each entry")

    # Set up state tracking so we dont reprocess old entries
    # There's no way to page the RSS feed so we have to track what we've seen
    # TODO - only do this at the end of the pipeline so we can retry records that fail
    processed_record_ids = dlt.current.resource_state().setdefault("processed_record_ids", [])
    feed = feedparser.parse(rss_feed_url)
    for entry in feed.entries:
        hashed_id = hashlib.sha256(entry.get("link").encode("utf-8")).hexdigest()

        if hashed_id in processed_record_ids:
            print(f"Skipping already processed entry with ID: {hashed_id}")
            continue
        else:
            print(f"Processing new entry with ID: {hashed_id}")
            processed_record_ids.append(hashed_id)
        yield {
            "id": hashed_id,
            "title": entry.get("title"),
            "summary": entry.get("summary"),
            "link": entry.get("link"),
            "published_at": entry.get("published"),
            "created_at": datetime.now()
        }

@dlt.transformer(table_name="jobs", data_from=rss_entries_resource, parallelized=True)
def get_company_name_from_rss_entry(job: dict):
    response = _get_open_ai_company_name(job["title"])
    yield {
        **job,
        "company_name": json.loads(response.output_text)["company"],
    }

@dlt.transformer(table_name="jobs", data_from=rss_entries_resource)
async def get_job_fit_score_from_rss_entry(job: dict):
    print(f"Processing fit for entry ID: {job['id']}")
    response = await _get_open_ai_fit_score(job["summary"])
    parsed = json.loads(response.output_text)
    yield {
        **job,
        "fit_score": parsed["fit_score"],
        "reasoning": parsed["reasoning"]
    }


def _get_open_ai_company_name(title: str):
    api_key = dlt.secrets.get("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model="gpt-5-nano",
        instructions="""
            Retrieve the company name from the input string. 
            If no company name is found return null. 
            You should behave like an API and return the string in JSON format. The key should always be `company`.
        """,
        input=title
    )
    return response
    
async def _get_open_ai_fit_score(summary: str):
    api_key = dlt.secrets.get("OPENAI_API_KEY")
    client = AsyncOpenAI(api_key=api_key)
    response = await client.responses.create(
        model="gpt-5-nano",
        instructions="""
            You are a recruiter at a company looking to fill a position.
 
            Carefully compare the job description input with the candidate's resume provided in the vector store. 
            Rank the resume's fit for the job on a scale of 1 to 10, with 10 being the best fit using whole numbers only. 
            


            - Only use information presented in the resume and job description to determine the fit score. 
            - Present brief reasoning for the score in 2-3 sentences. 

            Return the result in JSON format with the following structure, make sure it is always a valid JSON structure with open and close brackets.:
            { "fit_score": <integer>, "reasoning": "<brief reasoning>" }
        """,
        input=summary,
        tools=[{
            "type": "file_search",
            "vector_store_ids": [dlt.secrets.get("openai.vector_store_id")]
        }]
    )
    return response