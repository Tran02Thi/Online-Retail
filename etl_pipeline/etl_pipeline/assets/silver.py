from dagster import asset, AssetIn, Output
import pandas as pd

COMPUTE_KIND = "Pandas"
LAYER = "silver"
SCHEMA = "ecom"

@asset(
    io_manager_key="minio_io_manager",
    ins={
        "bronze_OnlineRetail_2010__2011": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description=" Data Cleansing all ecom files"
)
def silver_online_retail_2010_2011(context, bronze_OnlineRetail_2010__2011) -> Output[pd.DataFrame]:

    # Xóa các giá trị null
    df_cleaning = Cleaning(bronze_OnlineRetail_2010__2011)
    df = Preparation(df_cleaning)
    return Output(
        df,
        metadata={
            "table": "silver_online_retail_2010_2011",
            "records count": len(df),
        }
    )


def Cleaning(df):

    # Xóa các giá trị null
    df = df.dropna()

    # chuyển đổi kiểu dữ liệu của cột 'CustomerID' thành kiểu chuỗi (string)
    df['InvoiceNo'] = df['InvoiceNo'].astype("string")
    df['StockCode'] = df['StockCode'].astype("string")
    df['Description'] = df['Description'].astype("string")
    df['Quantity'] = df['Quantity'].astype(int)
    df['UnitPrice'] = df['UnitPrice'].astype(float)
    df['CustomerID'] = df['CustomerID'].astype(float)
    df['Country'] = df['Country'].astype("string")
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%d-%m-%Y %H:%M')

    return df

def Preparation(df):
    """
    desc:
        Phân tích Khách hàng dựa trên 3 yếu tố sau:
            R(Recency): Số ngày kể từ lần mua gần nhất ( tính đến 09/12/2011 ).
            F(Frequency): Số lần giao dịch.
            M(Monetary): Tổng số tiền giao dịch (doanh số đóng góp).
    :param df
    :return: df
    """


    # TODO: Monetary: Tổng số tiền giao dịch (doanh số đóng góp)
    df['Monetary'] = df['Quantity'] * df['UnitPrice']
    rt_m = df.groupby('CustomerID')['Monetary'].sum()
    rt_m = rt_m.reset_index()

    # TODO: F(Frequency): Tổng số lần giao dịch
    rt_f = df.groupby('CustomerID')['InvoiceNo'].count()
    rt_f = rt_f.reset_index()
    rt_f.columns = ['CustomerID', 'Frequency']

    # TODO: R(Recency): Khoảng thời gian từ lần giao dịch gần nhất đến hiện tại
    # Tính ngày giao dịch của lần giao dịch cuối cùng
    max_date = max(df['InvoiceDate'])
    df['Recency'] = max_date - df['InvoiceDate']
    rt_r = df.groupby('CustomerID')['Recency'].min()
    rt_r = rt_r.reset_index()
    rt_r['Recency'] = rt_r['Recency'].dt.days

    # TODO: Merge thành RFM dataframe
    mf = pd.merge(rt_m, rt_f, on='CustomerID', how='inner')
    rfm = pd.merge(mf, rt_r, on='CustomerID', how='inner')
    rfm.columns = ['CustomerID', 'Monetary', 'Frequency', 'Recency']

    return rfm























