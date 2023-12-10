from dagster import asset, AssetIn, Output
import pandas as pd

COMPUTE_KIND = "Postgres"
LAYER = "warehouse"

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "gold_online_retail_2010_2011": AssetIn(key_prefix=["gold", "ecom"]),
    },
    key_prefix=["public"],
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def pickup_cluster_label(context, gold_online_retail_2010_2011) -> Output[pd.DataFrame]:

    return Output(
        gold_online_retail_2010_2011,
        metadata={
            "table": "pickup_location_lat_lon",
            "records count": len(gold_online_retail_2010_2011)
        }
    )