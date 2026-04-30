#!/usr/bin/env python3
"""Push config/tasks.json → Notion Tasks DB.

Idempotent. Safe to run repeatedly. Uses Lead ID as the join key:
  - new records in tasks.json → created in Notion
  - existing records → updated (status, due, priority, confidence, reason)
  - Notion-only pages (manually added) → left alone

Requires NOTION_TASKS_DB_ID in .env. If the DB doesn't exist yet, run:
  python3 scripts/bootstrap_notion_tasks_db.py <parent_page_id>

Usage:
    python3 scripts/sync_tasks_to_notion.py           # full sync
    python3 scripts/sync_tasks_to_notion.py --dry-run # preview counts
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.notion import sync_tasks_from_json


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Count creates/updates without writing")
    ap.add_argument("--tasks-file", default=None,
                    help="Override path to tasks.json")
    args = ap.parse_args()

    result = sync_tasks_from_json(
        tasks_json_path=args.tasks_file,
        dry_run=args.dry_run,
    )
    if result.get("errors"):
        print(f"\n{len(result['errors'])} error(s):")
        for e in result["errors"][:10]:
            print(f"  - {e}")
    return 0 if not result.get("errors") else 1


if __name__ == "__main__":
    raise SystemExit(main())
