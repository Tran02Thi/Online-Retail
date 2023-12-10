from dagster import asset, AssetIn, Output
import pandas as pd
from sklearn.preprocessing import StandardScaler
from scipy.cluster.hierarchy import linkage
from scipy.cluster.hierarchy import cut_tree
COMPUTE_KIND = "Pandas"
LAYER = "gold"
SCHEMA = "ecom"

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_online_retail_2010_2011": AssetIn(key_prefix=["silver", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Outlier Analysis and Rescaling the Attributes"
)
def gold_online_retail_2010_2011(context, silver_online_retail_2010_2011) -> Output[pd.DataFrame]:
    df_remove_outlier = remove_outlier(silver_online_retail_2010_2011)
    df_rescaling = rescaling(df_remove_outlier)
    df_HierachicalClustering = HierachicalClustering(df_rescaling)
    df = cut_tree_dendrogram(df_HierachicalClustering, df_remove_outlier)

    return Output(
        df,
        metadata={
            "table": "silver_online_retail_2010_2011",
            "records count": len(df),
        }
    )


def find_outliers_IQR(df, tag):
    # Tạo hàm xóa outliers sử dụng IQR
    Q1 = df[tag].quantile(0.25)

    Q3 = df[tag].quantile(0.75)

    IQR = Q3 - Q1

    outliers = df[(df[tag] >= Q1 - 1.5 * IQR) & (df[tag] <= Q3 + 1.5 * IQR)]

    return outliers

def remove_outlier(df):
    df = find_outliers_IQR(df, "Monetary")
    df = find_outliers_IQR(df, "Recency")
    df = find_outliers_IQR(df, "Frequency")
    return df


def rescaling(df):
    rfm_df = df[['Monetary', 'Frequency', 'Recency']]

    # Instantiate
    scaler = StandardScaler()

    # fit_transform
    rfm_df_scaled = scaler.fit_transform(rfm_df)
    return rfm_df_scaled

def HierachicalClustering(df):
    mergings = linkage(df, method="complete", metric='euclidean')
    return mergings

def cut_tree_dendrogram(mergings, df):
    cluster_labels = cut_tree(mergings,  n_clusters=[4]).reshape(-1, )
    df['Cluster_Labels'] = cluster_labels
    return df


















