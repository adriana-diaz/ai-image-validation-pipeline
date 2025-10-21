
"""
storage_demo.py — Step-by-step, documented example for accessing Azure Storage
------------------------------------------------------------------------------
Purpose:
  Demonstrates how to (1) configure logging and environment variables,
  (2) connect to Azure Blob and Table Storage using a connection string,
  (3) ensure the container/table exist, (4) upload a blob (image),
  (5) write a metadata entity with PartitionKey/RowKey, and (6) perform
  batch inserts for efficiency.

Requirements (install):
  pip install azure-storage-blob azure-data-tables python-dotenv

Environment variables (recommended: .env locally, secrets in CI/prod):
  AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=...
  AZURE_BLOB_CONTAINER=image-input
  AZURE_TABLE_NAME=imageMetadata

Notes:
  - This is a generic, non-proprietary example for documentation purposes.
"""

# -----------------------------
# 0) Imports & global constants
# -----------------------------
import os
import uuid
import logging
import datetime as dt
from dataclasses import dataclass, asdict

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.data.tables import (
    TableServiceClient,
    TableClient,
    TableEntity,
    UpdateMode,
    TableTransactionAction,
)
from azure.core.exceptions import AzureError

# -------------------------------------
# 1) Logging setup (one-time per script)
# -------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("storage-demo")


# ----------------------------------------------------------------
# 2) Load environment and read connection string / basic settings
# ----------------------------------------------------------------
load_dotenv()  # loads .env if present (local dev)
CONN_STR   = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER  = os.getenv("AZURE_BLOB_CONTAINER", "image-input")
TABLE_NAME = os.getenv("AZURE_TABLE_NAME", "imageMetadata")

if not CONN_STR:
    raise RuntimeError("Missing AZURE_STORAGE_CONNECTION_STRING environment variable.")


# ------------------------------------------------------
# 3) Create service clients (Blob + Table) with handling
# ------------------------------------------------------
try:
    blob_service  = BlobServiceClient.from_connection_string(CONN_STR)
    table_service = TableServiceClient.from_connection_string(CONN_STR)
    log.info("Azure clients created.")
except Exception as e:
    log.exception("Failed to create Azure clients.")
    raise

# Container: create if not exists
container_client = blob_service.get_container_client(CONTAINER)
try:
    container_client.create_container()
    log.info("Blob container created: %s", CONTAINER)
except Exception:
    log.info("Blob container exists: %s", CONTAINER)

# Table: create if not exists
try:
    table_service.create_table_if_not_exists(TABLE_NAME)
    log.info("Table ensured: %s", TABLE_NAME)
except Exception as e:
    log.error("Failed to create/ensure table: %s", e)
    raise

table_client: TableClient = table_service.get_table_client(TABLE_NAME)


# -----------------------------------------------------------------------
# 4) Define a base metadata schema and helpers (unique Partition/Row keys)
# -----------------------------------------------------------------------
@dataclass
class ValidationRecord:
    """Schema for a validation record to be stored in Table Storage."""
    imageName: str
    captureTs: str         # ISO 8601 UTC (e.g., 2025-01-01T12:34:56Z)
    model: str
    inferenceValue: str    # store as string to keep it general
    confidence: float
    status: str            # e.g., 'validated', 'rejected', 'pending'
    # Optional extra fields could be added here as needed (e.g., siteId, notes)

def build_entity_from_record(rec: ValidationRecord) -> TableEntity:
    """Create a TableEntity with unique keys and normalized fields."""
    # Partition by day; adjust to your needs (site, line, etc.).
    # The captureTs is expected in UTC ISO format with a trailing 'Z'.
    parsed_ts = dt.datetime.fromisoformat(rec.captureTs.replace("Z", "+00:00"))
    entity: TableEntity = {
        "PartitionKey": parsed_ts.strftime("%Y%m%d"),
        "RowKey": str(uuid.uuid4()),
        **asdict(rec),
    }
    return entity


# ----------------------------------------------------------
# 5) Upload an image blob and log a single metadata entity
# ----------------------------------------------------------
def upload_image_and_log(path: str,
                         model: str,
                         inference_value,
                         confidence: float,
                         status: str = "validated") -> TableEntity:
    """Upload a local file as blob and write one metadata entity to Table."""
    blob_name = os.path.basename(path)

    # 5.1 Upload blob (idempotent overwrite for re-runs)
    with open(path, "rb") as fh:
        container_client.upload_blob(
            name=blob_name,
            data=fh,
            overwrite=True,
            content_settings=ContentSettings(content_type=_guess_mime(blob_name)),
        )
    log.info("Uploaded blob: %s", blob_name)

    # 5.2 Create and upsert entity
    now_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rec = ValidationRecord(
        imageName=blob_name,
        captureTs=now_utc,
        model=model,
        inferenceValue=str(inference_value),
        confidence=float(confidence),
        status=status,
    )
    entity = build_entity_from_record(rec)
    _safe_upsert(entity)
    log.info("Logged entity: PK=%s RK=%s", entity["PartitionKey"], entity["RowKey"])
    return entity


# --------------------------------------
# 6) Batch insert/update (Table Storage)
# --------------------------------------
def batch_log_entities(entities: list[TableEntity]):
    """Submit entities in batches, grouped by PartitionKey (<=100 ops/tx)."""
    groups: dict[str, list[TableEntity]] = {}
    for e in entities:
        groups.setdefault(e["PartitionKey"], []).append(e)

    for pk, group in groups.items():
        # chunk to max 100 actions per transaction
        actions = [TableTransactionAction(operation="upsert", entity=e) for e in group]
        for i in range(0, len(actions), 100):
            chunk = actions[i:i+100]
            resp = table_client.submit_transaction(chunk)
            log.info("Batch upsert: pk=%s, count=%d, response=%s", pk, len(chunk), resp)


# -----------------------------------
# 7) Helpers: MIME, safe upsert, etc.
# -----------------------------------
def _guess_mime(name: str) -> str:
    ext = name.lower().split(".")[-1]
    return {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "bmp": "image/bmp",
        "tif": "image/tiff",
        "tiff": "image/tiff",
        "webp": "image/webp",
    }.get(ext, "application/octet-stream")

def _safe_upsert(entity: TableEntity):
    try:
        table_client.upsert_entity(entity, mode=UpdateMode.MERGE)
    except AzureError as e:
        log.error("Table upsert failed: %s | RK=%s", e, entity.get("RowKey"))
        raise


# ---------------------------------------------------------
# 8) End-to-end example (run this file directly to test)
# ---------------------------------------------------------
if __name__ == "__main__":
    # Place a sample file under ./sample_images/ before running
    sample_path = os.path.join("sample_images", "image_ok.jpg")
    if not os.path.exists(sample_path):
        log.warning("Sample file not found: %s (demo will skip upload)", sample_path)
    else:
        entity = upload_image_and_log(
            path=sample_path,
            model="gauge_reader_v1",
            inference_value="123.4",
            confidence=0.94,
            status="validated",
        )
        log.info("Done. Entity keys -> PK=%s RK=%s", entity["PartitionKey"], entity["RowKey"])
