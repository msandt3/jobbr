import dlt
from sources.rss_feed import rss_feed_source
from sources.rss_feed import get_company_name_from_rss_entry, get_job_fit_score_from_rss_entry

def main():
    try:
        rss_url = dlt.secrets.get("REMOTE_FEED_URL")
    except FileNotFoundError:
        print("Configuration file not found. Please ensure .dlt/config.toml exists.")
        return
    except KeyError:
        print("job_feed_url not found in configuration file.")
        return
    

    pipeline = dlt.pipeline(
        pipeline_name="remote_jobs_pipeline",
        destination="duckdb",
    )

    pipeline.run(rss_feed_source(rss_url) | get_company_name_from_rss_entry | get_job_fit_score_from_rss_entry)

if __name__ == "__main__":
    main()