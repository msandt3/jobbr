import dlt
from duckdb import duckdb
from sources.duckdb_source import duckdb_source
from destinations.email_destination import email_destination

def main():
    pipeline = dlt.pipeline(
        pipeline_name="jobs_to_stdout",
        destination=email_destination
    )

    pipeline.run([
        duckdb_source(schema_name="atlanta_jobs", table_name="jobs")
        , duckdb_source(schema_name="remote_jobs", table_name="jobs")
    ])

if __name__ == "__main__":
    main()