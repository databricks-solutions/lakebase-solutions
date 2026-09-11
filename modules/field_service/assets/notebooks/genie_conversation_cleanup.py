# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Genie Conversation Cleanup
# MAGIC
# MAGIC Trims old conversations from each configured Genie space so they stay well
# MAGIC under Genie's **10,000-conversations-per-space** limit. For every space id
# MAGIC passed in, it lists conversations and deletes any whose last activity is
# MAGIC older than the retention window.
# MAGIC
# MAGIC The AI Supervisor and Genie AI pages create a conversation per question, so
# MAGIC without periodic cleanup a busy demo space eventually hits the cap and new
# MAGIC conversations start failing.
# MAGIC
# MAGIC **Schedule this as a recurring Databricks job** (e.g. daily). It is
# MAGIC idempotent and discovery-based — it enumerates the current conversations
# MAGIC from the API each run, so re-running is always safe.
# MAGIC
# MAGIC ### Parameters (job base_parameters / widgets)
# MAGIC - `space_ids` — comma-separated Genie space ids to clean
# MAGIC - `retention_days` — delete conversations older than this many days (default 30)

# COMMAND ----------

import time
from datetime import datetime, timezone, timedelta

from databricks.sdk import WorkspaceClient

# COMMAND ----------

dbutils.widgets.text("space_ids", "")
dbutils.widgets.text("retention_days", "30")

SPACE_IDS = [s.strip() for s in dbutils.widgets.get("space_ids").split(",") if s.strip()]
RETENTION_DAYS = int(dbutils.widgets.get("retention_days") or "30")

if not SPACE_IDS:
    raise ValueError("No space_ids provided — nothing to clean up.")

cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
print(f"Retention: {RETENTION_DAYS} days (cutoff {cutoff.isoformat()})")
print(f"Spaces: {SPACE_IDS}")

w = WorkspaceClient()

# COMMAND ----------


def _list_all_conversations(space_id):
    """Page through every conversation summary in a space."""
    out = []
    token = None
    while True:
        resp = w.genie.list_conversations(space_id, page_token=token)
        out.extend(resp.conversations or [])
        token = getattr(resp, "next_page_token", None)
        if not token:
            break
    return out


def cleanup_space(space_id):
    """Delete conversations in one space older than the cutoff. Returns (deleted, kept)."""
    deleted = kept = 0
    try:
        conversations = _list_all_conversations(space_id)
    except Exception as e:
        print(f"  [{space_id}] list_conversations failed: {e}")
        return 0, 0

    for conv in conversations:
        conv_id = getattr(conv, "conversation_id", None)
        if not conv_id:
            continue
        # ConversationSummary exposes created_timestamp (ms since epoch). Age by
        # creation is the right retention basis. Keep if timestamp is unknown.
        ts_ms = getattr(conv, "created_timestamp", None)
        if not ts_ms:
            kept += 1
            continue
        created = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        if created < cutoff:
            try:
                w.genie.delete_conversation(space_id, conv_id)
                deleted += 1
            except Exception as e:
                print(f"  [{space_id}] delete {conv_id} failed: {e}")
        else:
            kept += 1
    print(f"  [{space_id}] deleted={deleted} kept={kept} total={len(conversations)}")
    return deleted, kept


# COMMAND ----------

total_deleted = total_kept = 0
for sid in SPACE_IDS:
    d, k = cleanup_space(sid)
    total_deleted += d
    total_kept += k

print("=" * 60)
print(f"Genie cleanup complete: deleted={total_deleted} kept={total_kept}")
print("=" * 60)
