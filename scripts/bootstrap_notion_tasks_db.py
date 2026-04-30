#!/usr/bin/env python3
"""Bootstrap a Notion Tasks database under a parent page.

One-time setup script. Creates the database with the schema expected by
core.notion.sync_tasks_from_json, then prints the DB ID to paste into .env
as NOTION_TASKS_DB_ID.

Prereq: the Notion integration (NOTION_API_KEY) must already be shared with
the parent page. Share it via Notion UI → page → … → Connections → add.

Usage:
    python3 scripts/bootstrap_notion_tasks_db.py <parent_page_id>
    python3 scripts/bootstrap_notion_tasks_db.py <parent_page_id> --title "SDR Tasks"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.notion import create_tasks_database


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("parent_page_id",
                    help="Notion page ID (or URL) to create the DB under. "
                         "Integration must be shared with this page.")
    ap.add_argument("--title", default="Tasks",
                    help="Database title (default: Tasks)")
    args = ap.parse_args()

    parent = args.parent_page_id
    if "notion.so" in parent:
        # pull the 32-hex id from the URL
        parent = parent.rstrip("/").split("-")[-1].split("?")[0]

    print(f"Creating Tasks database under parent {parent} …")
    try:
        db = create_tasks_database(parent, title=args.title)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    db_id = db.get("id") or ""
    url = db.get("url") or ""
    print()
    print("✓ Database created.")
    print(f"  ID:  {db_id}")
    print(f"  URL: {url}")
    print()
    print("Next step — add this line to .env:")
    print(f"  NOTION_TASKS_DB_ID={db_id}")
    print()
    print("Then backfill with:")
    print("  python3 scripts/sync_tasks_to_notion.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
