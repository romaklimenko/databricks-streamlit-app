import os
from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

REQUIRED_ENV = ["DATABRICKS_WAREHOUSE_ID"]


def _assert_env() -> None:
    for v in REQUIRED_ENV:
        if not os.getenv(v):
            raise RuntimeError(f"{v} must be set")


def run_sql(query: str) -> pd.DataFrame:
    _assert_env()
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()
