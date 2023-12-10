from dagster import Definitions, load_assets_from_modules
from .resources.minio_io_manager import MinIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from . import assets
from dotenv import load_dotenv
import os
# load file .env to get
load_dotenv()

#TODO: MinIO
MINIO_CONFIG = {
    "endpoint_url": os.getenv('ENDPOINT_URL'),
    "bucket": os.getenv('BUCKET'),
    "aws_access_key_id": os.getenv('AWS_ACCESS_KEY_ID'),
    "aws_secret_access_key": os.getenv('AWS_SECRET_ACCESS_KEY')
}

PSQL_CONFIG = {
    "host": os.getenv('POSTGRES_HOST'),
    "port": os.getenv('POSTGRES_PORT'),
    "database": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
}
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "minio_io_manager": MinIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG)
    }
)
