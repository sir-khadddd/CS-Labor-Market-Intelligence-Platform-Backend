"""
Source configuration for Revelio extracts.
Mirrors the cloner-style product/table map and partition columns.
"""

from dataclasses import dataclass
from pathlib import Path
import os

from .backend_env import load_backend_env

load_backend_env()


@dataclass(frozen=True)
class TableConfig:
    product: str
    table: str
    partition_column: str
    partition_type: str = "month"


REVELIO_TABLES = {
    "revelio_job_postings": {
        "schema": "revelio_job_postings",
        "tables": {
            "postings_cosmos": {"partition_column": "post_date", "partition_type": "month"},
        },
    },
    "revelio_workforce_dynamics": {
        "schema": "revelio_workforce_dynamics",
        "tables": {
            "workforce_dynamics": {"partition_column": "date", "partition_type": "month"},
        },
    },
    "revelio_sentiment": {
        "schema": "revelio_sentiment",
        "tables": {
            "sentiment": {"partition_column": "date", "partition_type": "month"},
        },
    },
}


def get_source_root() -> Path:
    raw = os.getenv("CS_LMI_REVELIO_ROOT", "data/raw_cs_snapshot")
    return Path(raw)


def get_duckdb_path() -> Path:
    raw = os.getenv("CS_LMI_DUCKDB_PATH", "data/local/cs_lmi.duckdb")
    return Path(raw)


def get_processed_dir() -> Path:
    raw = os.getenv("CS_LMI_PROCESSED_DIR", "data/processed")
    return Path(raw)


def iter_table_configs():
    for product, product_cfg in REVELIO_TABLES.items():
        for table, table_cfg in product_cfg["tables"].items():
            yield TableConfig(
                product=product,
                table=table,
                partition_column=table_cfg["partition_column"],
                partition_type=table_cfg.get("partition_type", "month"),
            )
