from sqlalchemy import create_engine
from minio import Minio
TYPE_CONNECTIONS = {}

def connect_minio(endpoint_url: str, aws_access_key_id: str, aws_secret_access_key: str) -> Minio:
    client = Minio (
                endpoint=endpoint_url,
                access_key=aws_access_key_id,
                secret_key=aws_secret_access_key,
                secure=False
            )
    return client

def connect_postgresql(host: str, port: int, user: str, password: str, database: str):
    conn_string = (
            f"postgresql+psycopg2://{user}:{password}"
            + f"@{host}:{port}"
            + f"/{database}"
    )
    return create_engine(conn_string)

def register_stored_connector(stored_type: str):
    def decorator(func):
        TYPE_CONNECTIONS[stored_type] = func
        return func
    return decorator



@register_stored_connector('postgresql')
def postgresql_connector(host, port, user, password, database):
    return connect_postgresql(host, port, user, password, database)

@register_stored_connector('minio')
def minio_connector(endpoint_url, aws_access_key_id, aws_secret_access_key):
    return connect_minio(endpoint_url, aws_access_key_id, aws_secret_access_key)

def get_db_connector(stored_type):
    if stored_type not in TYPE_CONNECTIONS:
        raise ValueError(f"Unsupported Type Connection: {stored_type}")
    return TYPE_CONNECTIONS[stored_type]

if __name__ == "__main__":
    print("Choose Type Stable")
