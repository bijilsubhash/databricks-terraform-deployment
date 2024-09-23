import dlt

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.destinations import filesystem

import pendulum

BASE_URL = "https://app.ticketmaster.com/{api_path}/"

def get_current_datetime() -> pendulum.DateTime:
    return pendulum.now()

def fetch_ticketmaster_data(
        api_path,
        endpoint,
        api_key,
        data_selector, 
        next_url_path,
        params={}
        ):
    client = RESTClient(
    base_url=BASE_URL.format(api_path=api_path),
    auth=APIKeyAuth(
        name="apikey",
        api_key=api_key, 
        location="query"),
    data_selector=data_selector,
    paginator=JSONLinkPaginator(next_url_path=next_url_path)
    )
    for page in client.paginate(endpoint, params=params):
        yield page

@dlt.source
def ticketmaster_source(
    api_key: str = dlt.secrets.value,
    api_path: str = dlt.config.value
    ):
    for table in ["events", "attractions", "venues"]:
        params = {"size": 30, "countryCode": "AU"}
        endpoint = f"{table}.json"
        data_selector = f"_embedded.{table}"
        next_url_path = "_links.next.href"
        yield dlt.resource(
            fetch_ticketmaster_data(
                api_path,
                endpoint,
                api_key,
                data_selector,
                next_url_path,
                params
                ),
            name=table,
            write_disposition="replace",
            max_table_nesting=0,
            parallelized=True
        )

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name='ticketmaster',
        dataset_name='ticketmaster_layout_test',
        destination=filesystem(
            layout="{table_name}/year={YYYY}/month={MMMM}/day={DD}/{load_id}.{file_id}.{ext}",
            current_datetime=get_current_datetime,
        ),
        progress="log"
    )

    load_info = pipeline.run(ticketmaster_source().add_limit(10)) #hard limit for dev
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print(load_info)
