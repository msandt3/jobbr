import dlt
from duckdb import duckdb
@dlt.source
def duckdb_source(schema_name: str, table_name: str):
    return get_jobs(schema_name, table_name)

@dlt.resource
def get_jobs(schema_name: str, table_name: str):
    con = duckdb.connect(database=dlt.secrets.get('destination.motherduck.credentials'), read_only=True)
    result = con.execute(f"SELECT title, link, company_name, fit_score, reasoning FROM {schema_name}.{table_name} order by fit_score desc limit 5").df().to_dict(orient='records')
    yield result
    con.close()


    
