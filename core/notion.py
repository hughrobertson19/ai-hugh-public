"""Notion integration module for AI Hugh.

Read helpers for seven databases — Deals, Meetings, Objections Library, Proof
Points, Competitive Intel, Companies, Contacts — plus write helpers for
appending objections and managing the master CRM (Companies + Contacts).

Notion's nested block structure is flattened into plain Python dicts and
strings so callers do not need to understand the API schema.

Environment (loaded from .env):
    NOTION_API_KEY                    — integration token
    NOTION_DEALS_DB_ID                — Deals database (formerly Demos)
    NOTION_MEETINGS_DB_ID             — Meetings database
    NOTION_OBJECTIONS_DB_ID           — Objections Library
    NOTION_PROOF_POINTS_DB_ID         — Proof Points
    NOTION_COMPETITIVE_INTEL_DB_ID    — Competitive Intel
    NOTION_SPRINTS_DB_ID              — Sprint Tracker
    NOTION_COMPANIES_DB_ID            — Companies (master CRM)
    NOTION_CONTACTS_DB_ID             — Contacts (master CRM)

Public functions:
    get_deal(company_name)            → dict | None (properties + body text)
    get_recent_deals(days=14)         → list[dict]
    get_meetings(type=None, status=None, days=14) → list[dict]
    get_meetings_for_deal(deal_name)  → list[dict] (linked via Deal relation)
    get_deal_context(deal_name)       → dict (deal + company + contacts + meetings + CI)
    get_open_actions()                → {"deals": [...], "meetings": [...]}
    search_across(query)              → {"deals": [...], "meetings": [...]}
    get_objections(category=None, stage=None) → list[dict]
    get_proof_points(vertical=None, product=None) → list[dict]
    get_competitor(name)              → dict | None (with body)
    get_competitors()                 → list[dict]
    add_objection(data)               → dict (created page)
    get_sprint(name)                  → dict | None (with body)
    get_active_sprint()               → dict | None (Status = Active)
    update_sprint_stats(name, stats)  → dict (updated page)

    # Master CRM — Companies
    get_companies(status=None, product=None, industry=None) → list[dict]
    get_company(name)                 → dict | None (with body)
    add_company(data)                 → dict (created page)
    update_company(page_id, data)     → dict (updated page)
    bulk_import_companies(csv_path)   → dict (counts)

    # Master CRM — Contacts
    get_contacts(company=None, role=None, status=None, campaign=None, product=None) → list[dict]
    get_contact(name)                 → dict | None (with body)
    add_contact(data)                 → dict (created page)
    update_contact(page_id, data)     → dict (updated page)
    bulk_import_contacts(csv_path)    → dict (counts)

    # Query helpers for outreach scripts
    get_contacts_by_area_code(area_code)    → list[dict]
    get_contacts_by_campaign(campaign_name) → list[dict]
    get_contacts_by_timezone(tz)            → list[dict]
    update_contact_status(name, status, method=None) → dict

CLI smoke test:
    python -m core.notion --test
    python -m core.notion --test-crm    # exercises Companies/Contacts round-trip
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

_client: Optional[Client] = None
_data_source_cache: dict[str, str] = {}


def _env(name: str) -> str:
    val = os.getenv(name) or ""
    if not val:
        raise RuntimeError(
            f"{name} not set in .env — add it before using core.notion"
        )
    return val


def _get_client() -> Client:
    global _client
    if _client is None:
        _client = Client(auth=_env("NOTION_API_KEY"))
    return _client


def _join_rich_text(items: list) -> str:
    return "".join((item.get("plain_text") or "") for item in items or [])


def _extract_property(prop: dict) -> Any:
    """Flatten one Notion property object into a plain Python value."""
    ptype = prop.get("type")
    if ptype in ("title", "rich_text"):
        return _join_rich_text(prop.get(ptype, []))
    if ptype == "date":
        d = prop.get("date")
        return {"start": d.get("start"), "end": d.get("end")} if d else None
    if ptype == "status":
        s = prop.get("status")
        return s.get("name") if s else None
    if ptype == "select":
        s = prop.get("select")
        return s.get("name") if s else None
    if ptype == "multi_select":
        return [o.get("name") for o in prop.get("multi_select", [])]
    if ptype == "checkbox":
        return bool(prop.get("checkbox"))
    if ptype == "number":
        return prop.get("number")
    if ptype in ("url", "email", "phone_number"):
        return prop.get(ptype)
    if ptype == "people":
        return [p.get("name") or p.get("id") for p in prop.get("people", [])]
    if ptype == "relation":
        return [r.get("id") for r in prop.get("relation", [])]
    if ptype == "files":
        return [f.get("name") for f in prop.get("files", [])]
    if ptype in ("created_time", "last_edited_time"):
        return prop.get(ptype)
    if ptype in ("created_by", "last_edited_by"):
        u = prop.get(ptype) or {}
        return u.get("name") or u.get("id")
    if ptype == "formula":
        f = prop.get("formula") or {}
        return f.get(f.get("type"))
    if ptype == "rollup":
        r = prop.get("rollup") or {}
        rtype = r.get("type")
        if rtype == "array":
            return [_extract_property(item) for item in r.get("array", [])]
        return r.get(rtype)
    return prop.get(ptype)


def _title_of(page: dict) -> str:
    """Return the text of the page's title property, whatever its name is."""
    for prop in (page.get("properties") or {}).values():
        if prop.get("type") == "title":
            return _join_rich_text(prop.get("title", []))
    return ""


def _page_to_dict(page: dict) -> dict:
    props = {
        name: _extract_property(prop)
        for name, prop in (page.get("properties") or {}).items()
    }
    return {
        "id": page.get("id"),
        "url": page.get("url"),
        "title": _title_of(page),
        "created_time": page.get("created_time"),
        "last_edited_time": page.get("last_edited_time"),
        "properties": props,
    }


def _block_to_text(block: dict) -> str:
    btype = block.get("type") or ""
    payload = block.get(btype) or {}
    text = _join_rich_text(payload.get("rich_text", []))
    if not text:
        return ""
    if btype.startswith("heading_"):
        level = int(btype.rsplit("_", 1)[-1] or 1)
        return "#" * level + " " + text
    if btype == "bulleted_list_item":
        return "• " + text
    if btype == "numbered_list_item":
        return "1. " + text
    if btype == "to_do":
        return ("[x] " if payload.get("checked") else "[ ] ") + text
    if btype == "quote":
        return "> " + text
    if btype == "callout":
        return "💡 " + text
    return text


def _get_page_body(page_id: str) -> str:
    """Flatten a page's top-level blocks into newline-joined plain text."""
    client = _get_client()
    lines: list[str] = []
    cursor: Optional[str] = None
    while True:
        kwargs = {"block_id": page_id}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.blocks.children.list(**kwargs)
        for block in resp.get("results", []):
            text = _block_to_text(block)
            if text:
                lines.append(text)
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    return "\n".join(lines)


def _resolve_data_source_id(database_or_source_id: str) -> str:
    """Notion API (2025) moved query from databases to data_sources. A database can host
    multiple data sources; for typical single-source DBs we grab the first. If the ID
    already refers to a data source (databases.retrieve fails), use it directly.
    """
    cached = _data_source_cache.get(database_or_source_id)
    if cached:
        return cached
    client = _get_client()
    try:
        db = client.databases.retrieve(database_id=database_or_source_id)
        sources = db.get("data_sources") or []
        if sources and sources[0].get("id"):
            ds_id = sources[0]["id"]
        else:
            ds_id = database_or_source_id
    except APIResponseError:
        ds_id = database_or_source_id
    _data_source_cache[database_or_source_id] = ds_id
    return ds_id


def _query_all(database_id: str, **query_kwargs) -> list[dict]:
    """Page through data_sources.query, returning all raw page objects."""
    client = _get_client()
    data_source_id = _resolve_data_source_id(database_id)
    out: list[dict] = []
    cursor: Optional[str] = None
    while True:
        kwargs = {"data_source_id": data_source_id, **query_kwargs}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = client.data_sources.query(**kwargs)
        out.extend(resp.get("results", []))
        if resp.get("has_more"):
            cursor = resp.get("next_cursor")
        else:
            break
    return out


def get_deal(company_name: str) -> Optional[dict]:
    """Return the most-recently-edited Deals entry whose title or Company/Account
    property matches company_name (case-insensitive substring). Includes body text.
    Returns None if no match.
    """
    needle = (company_name or "").strip().lower()
    if not needle:
        return None
    pages = _query_all(_env("NOTION_DEALS_DB_ID"))
    matches: list[dict] = []
    for page in pages:
        entry = _page_to_dict(page)
        haystacks = [entry["title"] or ""]
        for key in ("Company", "Account", "Customer"):
            val = entry["properties"].get(key)
            if isinstance(val, str):
                haystacks.append(val)
        if any(needle in (h or "").lower() for h in haystacks):
            matches.append(entry)
    if not matches:
        return None
    matches.sort(key=lambda e: e.get("last_edited_time") or "", reverse=True)
    top = matches[0]
    top["body"] = _get_page_body(top["id"])
    return top


def get_recent_deals(days: int = 14) -> list[dict]:
    """Return Deals entries edited in the last `days` days, newest first."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    pages = _query_all(
        _env("NOTION_DEALS_DB_ID"),
        filter={
            "timestamp": "last_edited_time",
            "last_edited_time": {"on_or_after": cutoff},
        },
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    return [_page_to_dict(p) for p in pages]


def get_meetings(
    type: Optional[str] = None,
    status: Optional[str] = None,
    days: int = 14,
) -> list[dict]:
    """Return Meetings entries edited in the last `days` days, newest first.
    Optional case-insensitive filters on Type and Status properties.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    pages = _query_all(
        _env("NOTION_MEETINGS_DB_ID"),
        filter={
            "timestamp": "last_edited_time",
            "last_edited_time": {"on_or_after": cutoff},
        },
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if type:
        t = type.lower()
        out = [e for e in out if (e["properties"].get("Type") or "").lower() == t]
    if status:
        s = status.lower()
        out = [e for e in out if (e["properties"].get("Status") or "").lower() == s]
    return out


def get_meetings_for_deal(deal_name: str) -> list[dict]:
    """Return Meetings whose 'Deal' relation contains the Deal matching
    `deal_name` (fuzzy title match). Returns [] if no Deal matches.
    """
    deal = get_deal(deal_name)
    if deal is None:
        return []
    deal_id = deal["id"]
    pages = _query_all(
        _env("NOTION_MEETINGS_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out: list[dict] = []
    for p in pages:
        entry = _page_to_dict(p)
        if deal_id in (entry["properties"].get("Deal") or []):
            out.append(entry)
    return out


def get_deal_context(deal_name: str) -> Optional[dict]:
    """Return the full context for a Deal in one dict — intended for "open a
    deal and see everything" use cases (e.g. call prep, handoff doc).

    Shape:
        {
            "deal":              <deal page dict with body>,
            "company":           <company page dict or None>,
            "contacts":          [<contact page dict>, ...],
            "meetings":          [<meeting page dict>, ...],
            "competitive_intel": [<CI page dict>, ...],
        }

    Company is resolved from the Deal's 'Company (linked)' relation if set,
    falling back to the legacy 'Related to Companies (Demos)' relation or the
    text 'Company' field (fuzzy name match).  Competitive Intel is pulled via
    the Company's 'Competitive Intel' relation.
    """
    deal = get_deal(deal_name)
    if deal is None:
        return None
    client = _get_client()
    props = deal["properties"]

    # --- Company ---
    company = None
    company_ids = props.get("Company (linked)") or props.get(
        "Related to Companies (Demos)"
    ) or []
    if company_ids:
        try:
            raw = client.pages.retrieve(page_id=company_ids[0])
            company = _page_to_dict(raw)
            company["body"] = _get_page_body(company["id"])
        except Exception as e:
            print(f"[notion] get_deal_context: company fetch — {type(e).__name__}: {e}",
                  file=sys.stderr)
    else:
        # Fall back to fuzzy match on the legacy text Company field
        text_name = props.get("Company")
        if isinstance(text_name, str) and text_name.strip():
            company = get_company(text_name)

    # --- Contacts ---
    contacts = []
    for cid in props.get("Contacts") or []:
        try:
            contacts.append(_page_to_dict(client.pages.retrieve(page_id=cid)))
        except Exception as e:
            print(f"[notion] get_deal_context: contact fetch — {type(e).__name__}: {e}",
                  file=sys.stderr)

    # --- Meetings ---
    meetings = []
    for mid in props.get("Meetings") or []:
        try:
            meetings.append(_page_to_dict(client.pages.retrieve(page_id=mid)))
        except Exception as e:
            print(f"[notion] get_deal_context: meeting fetch — {type(e).__name__}: {e}",
                  file=sys.stderr)

    # --- Competitive Intel (via Company) ---
    ci: list[dict] = []
    if company:
        for cid in (company["properties"].get("Competitive Intel") or []):
            try:
                ci.append(_page_to_dict(client.pages.retrieve(page_id=cid)))
            except Exception as e:
                print(f"[notion] get_deal_context: CI fetch — {type(e).__name__}: {e}",
                      file=sys.stderr)

    return {
        "deal": deal,
        "company": company,
        "contacts": contacts,
        "meetings": meetings,
        "competitive_intel": ci,
    }


def get_open_actions() -> dict:
    """Return entries from Deals + Meetings whose 'Open Actions?' checkbox is True.

    Previously filtered on a 'Status' select/status property, but the checkbox
    is the signal that actually gets set consistently across deal and meeting
    entries (including entries created via process_meeting.py). DBs without
    the checkbox simply return no matches.
    """
    out = {"deals": [], "meetings": []}
    for key, env_name in (
        ("deals", "NOTION_DEALS_DB_ID"),
        ("meetings", "NOTION_MEETINGS_DB_ID"),
    ):
        db_id = os.getenv(env_name)
        if not db_id:
            continue
        for page in _query_all(db_id):
            entry = _page_to_dict(page)
            if entry["properties"].get("Open Actions?") is True:
                out[key].append(entry)
    return out


def search_across(query: str) -> dict:
    """Substring-match `query` against string and list-of-string properties
    in Deals + Meetings. Returns {"deals": [...], "meetings": [...]}.
    """
    q = (query or "").strip().lower()
    out = {"deals": [], "meetings": []}
    if not q:
        return out
    for key, env_name in (
        ("deals", "NOTION_DEALS_DB_ID"),
        ("meetings", "NOTION_MEETINGS_DB_ID"),
    ):
        db_id = os.getenv(env_name)
        if not db_id:
            continue
        for page in _query_all(db_id):
            entry = _page_to_dict(page)
            hit = q in (entry["title"] or "").lower()
            if not hit:
                for v in entry["properties"].values():
                    if isinstance(v, str) and q in v.lower():
                        hit = True
                        break
                    if isinstance(v, list) and any(
                        isinstance(x, str) and q in x.lower() for x in v
                    ):
                        hit = True
                        break
            if hit:
                out[key].append(entry)
    return out


def get_objections(
    category: Optional[str] = None,
    stage: Optional[str] = None,
) -> list[dict]:
    """Return Objections entries, optionally filtered by Category or Stage raised.
    Case-insensitive match against single-select Category and any value in
    multi-select Stage raised.
    """
    pages = _query_all(
        _env("NOTION_OBJECTIONS_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if category:
        cat = category.lower()
        out = [
            e
            for e in out
            if (e["properties"].get("Category") or "").lower() == cat
        ]
    if stage:
        s = stage.lower()
        out = [
            e
            for e in out
            if any(
                (x or "").lower() == s
                for x in (e["properties"].get("Stage raised") or [])
            )
        ]
    return out


def get_proof_points(
    vertical: Optional[str] = None,
    product: Optional[str] = None,
) -> list[dict]:
    """Return Proof Points entries, optionally filtered by Vertical or UL Product.
    Both are multi-selects, so match is any-of (case-insensitive)."""
    pages = _query_all(
        _env("NOTION_PROOF_POINTS_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if vertical:
        v = vertical.lower()
        out = [
            e
            for e in out
            if any(
                (x or "").lower() == v
                for x in (e["properties"].get("Vertical") or [])
            )
        ]
    if product:
        p = product.lower()
        out = [
            e
            for e in out
            if any(
                (x or "").lower() == p
                for x in (e["properties"].get("UL Product") or [])
            )
        ]
    return out


def get_competitor(name: str) -> Optional[dict]:
    """Return the Competitive Intel entry whose Competitor title matches `name`
    (case-insensitive substring). Includes page body. Returns None if no match.
    """
    needle = (name or "").strip().lower()
    if not needle:
        return None
    pages = _query_all(_env("NOTION_COMPETITIVE_INTEL_DB_ID"))
    matches = [
        _page_to_dict(p)
        for p in pages
        if needle in (_title_of(p) or "").lower()
    ]
    if not matches:
        return None
    matches.sort(key=lambda e: e.get("last_edited_time") or "", reverse=True)
    top = matches[0]
    top["body"] = _get_page_body(top["id"])
    return top


def get_competitors() -> list[dict]:
    """Return all Competitive Intel entries (properties only, no body)."""
    pages = _query_all(
        _env("NOTION_COMPETITIVE_INTEL_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    return [_page_to_dict(p) for p in pages]


def _rich_text(text: str) -> list[dict]:
    """Build a minimal rich_text array from a plain string (2000-char Notion limit)."""
    if not text:
        return []
    return [{"type": "text", "text": {"content": text[:2000]}}]


def _body_blocks(body_markdown: str) -> list[dict]:
    """Turn a simple markdown string into Notion block children.
    Supports '## heading', '- bullets', blank-line-separated paragraphs. Anything
    fancier should be authored in Notion directly.
    """
    if not body_markdown:
        return []
    blocks: list[dict] = []
    for line in body_markdown.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("## "):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {"rich_text": _rich_text(stripped[3:])},
                }
            )
        elif stripped.startswith("# "):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {"rich_text": _rich_text(stripped[2:])},
                }
            )
        elif stripped.startswith(("- ", "• ")):
            blocks.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": _rich_text(stripped[2:])},
                }
            )
        else:
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _rich_text(stripped)},
                }
            )
    return blocks


def add_objection(data: dict) -> dict:
    """Create a new entry in the Objections Library.

    `data` keys (all optional except `objection`):
        objection        (str)        — title
        category         (str)        — Budget / Timing / Incumbent / Authority / Need / Trust / Process
        stage            (list[str])  — Cold / Discovery / Demo / Procurement / Post-proposal
        response_script  (str)
        key_reframe      (str)
        frequency        (int)
        win_rate         (float, 0–1) — Win rate when used (percent)
        last_encountered (str ISO date) — defaults to today
        notes_body       (str)        — markdown body ("##" headings, "-" bullets)

    Returns the created Notion page object (raw).
    """
    client = _get_client()
    db_id = _env("NOTION_OBJECTIONS_DB_ID")
    data_source_id = _resolve_data_source_id(db_id)

    title_text = (data.get("objection") or "").strip()
    if not title_text:
        raise ValueError("add_objection requires 'objection' (title)")

    props: dict = {"Objection": {"title": _rich_text(title_text)}}
    if data.get("category"):
        props["Category"] = {"select": {"name": data["category"]}}
    if data.get("stage"):
        stages = data["stage"] if isinstance(data["stage"], list) else [data["stage"]]
        props["Stage raised"] = {
            "multi_select": [{"name": s} for s in stages if s]
        }
    if data.get("response_script"):
        props["Response script"] = {"rich_text": _rich_text(data["response_script"])}
    if data.get("key_reframe"):
        props["Key reframe"] = {"rich_text": _rich_text(data["key_reframe"])}
    if data.get("frequency") is not None:
        props["Frequency"] = {"number": int(data["frequency"])}
    if data.get("win_rate") is not None:
        # Notion percent format expects 0..1
        wr = float(data["win_rate"])
        if wr > 1:
            wr = wr / 100.0
        props["Win rate when used"] = {"number": wr}
    last = data.get("last_encountered") or datetime.now(timezone.utc).date().isoformat()
    props["Last encountered"] = {"date": {"start": last}}

    kwargs = {
        "parent": {"type": "data_source_id", "data_source_id": data_source_id},
        "properties": props,
    }
    body = _body_blocks(data.get("notes_body") or "")
    if body:
        kwargs["children"] = body
    return client.pages.create(**kwargs)


def get_sprint(name: str) -> Optional[dict]:
    """Return the Sprint Tracker entry whose Sprint name matches `name`
    (case-insensitive substring). Includes page body. Returns None if no match.
    """
    needle = (name or "").strip().lower()
    if not needle:
        return None
    pages = _query_all(_env("NOTION_SPRINTS_DB_ID"))
    matches = [
        _page_to_dict(p)
        for p in pages
        if needle in (_title_of(p) or "").lower()
    ]
    if not matches:
        return None
    matches.sort(key=lambda e: e.get("last_edited_time") or "", reverse=True)
    top = matches[0]
    top["body"] = _get_page_body(top["id"])
    return top


def get_active_sprint() -> Optional[dict]:
    """Return the Sprint Tracker entry whose Status = 'Active'. If multiple are
    Active, returns the most recently edited. Includes page body.
    """
    pages = _query_all(_env("NOTION_SPRINTS_DB_ID"))
    actives = []
    for p in pages:
        entry = _page_to_dict(p)
        if (entry["properties"].get("Status") or "").lower() == "active":
            actives.append(entry)
    if not actives:
        return None
    actives.sort(key=lambda e: e.get("last_edited_time") or "", reverse=True)
    top = actives[0]
    top["body"] = _get_page_body(top["id"])
    return top


# Mapping of friendly stat keys → Notion property name + type.
# Used by update_sprint_stats to build a minimal properties payload.
_SPRINT_NUMERIC_FIELDS = {
    "lead_pool_size": ("Lead pool size", "number"),
    "total_sent": ("Total sent", "number"),
    "bounce_rate": ("Bounce rate", "percent"),
    "reply_rate": ("Reply rate", "percent"),
    "demos_booked": ("Demos booked", "number"),
    "opps_created": ("Opps created", "number"),
}


def update_sprint_stats(name: str, stats: dict) -> dict:
    """Update numeric fields on the sprint whose Sprint name matches `name`.

    `stats` accepts any of: lead_pool_size, total_sent, bounce_rate, reply_rate,
    demos_booked, opps_created. Rate fields accept either 0..1 (e.g. 0.02) or
    percent values (e.g. 2.0 for 2%). Notes field also supported via "notes".

    Raises ValueError if no sprint matches `name`.
    """
    sprint = get_sprint(name)
    if not sprint:
        raise ValueError(f"No sprint found with name containing {name!r}")
    client = _get_client()
    props: dict = {}
    for key, value in (stats or {}).items():
        if key == "notes" and value is not None:
            props["Notes"] = {"rich_text": _rich_text(str(value))}
            continue
        mapping = _SPRINT_NUMERIC_FIELDS.get(key)
        if mapping is None:
            continue
        prop_name, kind = mapping
        if value is None:
            continue
        if kind == "percent":
            v = float(value)
            if v > 1:
                v = v / 100.0
            props[prop_name] = {"number": v}
        else:
            props[prop_name] = {"number": float(value) if not isinstance(value, int) else int(value)}
    if not props:
        return sprint
    return client.pages.update(page_id=sprint["id"], properties=props)


def sync_sprint_stats_from_log(sent_log_path: str, sprint_name: Optional[str] = None) -> dict:
    """Recount a sent_log.csv and push Total sent + Bounce rate to the Sprint
    Tracker entry for `sprint_name`. If `sprint_name` is None, the active
    sprint (Status = Active) is used.

    Never raises. On any failure (log missing, malformed, Notion unreachable,
    no matching sprint), logs to stderr and returns {}. Callers should invoke
    this after a send batch and simply ignore the return value.

    The count is conservative:
      total_sent  = count(status in {sent, bounced})  — attempted SMTP sends
      bounce_rate = bounced / total_sent              — as a percent

    Assumes the sent_log.csv schema is 7 cols: timestamp, email, first_name,
    company, subject, status, error. Same schema as `load_already_sent()`.
    Rows that don't match aren't counted.
    """
    import csv as _csv
    try:
        total = 0
        bounced = 0
        with open(sent_log_path, newline="") as f:
            reader = _csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if not row or len(row) < 6:
                    continue
                status = (row[5] or "").strip().lower()
                if status == "sent":
                    total += 1
                elif status == "bounced":
                    total += 1
                    bounced += 1
    except FileNotFoundError:
        print(f"[notion] sync_sprint_stats: log not found at {sent_log_path}", file=sys.stderr)
        return {}
    except Exception as e:
        print(
            f"[notion] sync_sprint_stats: could not read log — {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return {}

    if total == 0:
        return {}

    # Resolve sprint name if not provided
    if sprint_name is None:
        try:
            active = get_active_sprint()
        except Exception as e:
            print(
                f"[notion] sync_sprint_stats: could not fetch active sprint — {type(e).__name__}: {e}",
                file=sys.stderr,
            )
            return {}
        if active is None:
            print(
                "[notion] sync_sprint_stats: no active sprint found — skipping",
                file=sys.stderr,
            )
            return {}
        sprint_name = active.get("title") or ""
        if not sprint_name:
            print(
                "[notion] sync_sprint_stats: active sprint has no title — skipping",
                file=sys.stderr,
            )
            return {}

    bounce_rate = bounced / total
    stats = {"total_sent": total, "bounce_rate": bounce_rate}
    try:
        update_sprint_stats(sprint_name, stats)
        print(
            f"[notion] sync_sprint_stats: total_sent={total}, "
            f"bounced={bounced} ({bounce_rate * 100:.1f}%) → {sprint_name!r}",
            file=sys.stderr,
        )
    except Exception as e:
        print(
            f"[notion] sync_sprint_stats: Notion update failed — {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return {}
    return stats


def safe(fn, *args, **kwargs):
    """Call a Notion function with stderr logging; returns None on failure.

    Scripts that enrich their output from Notion should wrap every call with
    this helper so a Notion outage or API error never breaks the parent
    workflow. Logs success (with result count) and failure (with exception
    type) to stderr so the user can see what happened without the output
    polluting the primary stdout stream.

        from core.notion import safe, get_deal
        deal = safe(get_deal, company_name)
    """
    name = getattr(fn, "__name__", "notion_call")
    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        print(f"[notion] {name}: FAILED — {type(e).__name__}: {e}", file=sys.stderr)
        return None
    if result is None:
        print(f"[notion] {name}: no match", file=sys.stderr)
    elif isinstance(result, list):
        print(f"[notion] {name}: {len(result)} result(s)", file=sys.stderr)
    elif isinstance(result, dict) and "deals" in result and "meetings" in result:
        print(
            f"[notion] {name}: deals={len(result['deals'])} meetings={len(result['meetings'])}",
            file=sys.stderr,
        )
    else:
        print(f"[notion] {name}: ok", file=sys.stderr)
    return result


def _cli_test() -> int:
    print("=" * 60)
    print("core.notion — smoke test")
    print("=" * 60)
    for name in (
        "NOTION_API_KEY",
        "NOTION_DEALS_DB_ID",
        "NOTION_MEETINGS_DB_ID",
        "NOTION_OBJECTIONS_DB_ID",
        "NOTION_PROOF_POINTS_DB_ID",
        "NOTION_COMPETITIVE_INTEL_DB_ID",
        "NOTION_SPRINTS_DB_ID",
    ):
        val = os.getenv(name) or ""
        print(f"  {name}: {'set' if val else 'MISSING'}")
    print()

    def _section(label: str, fn) -> None:
        print(f"— {label} —")
        try:
            result = fn()
            if isinstance(result, list):
                print(f"  count: {len(result)}")
                for entry in result[:3]:
                    t = entry.get("title") or "(untitled)"
                    le = entry.get("last_edited_time") or ""
                    print(f"    • {t[:60]}  [edited {le[:10]}]")
            elif isinstance(result, dict) and "deals" in result:
                print(
                    f"  deals={len(result['deals'])}  meetings={len(result['meetings'])}"
                )
                for bucket in ("deals", "meetings"):
                    for entry in result[bucket][:2]:
                        t = entry.get("title") or "(untitled)"
                        print(f"    • [{bucket}] {t[:60]}")
            else:
                print(f"  result type: {type(result).__name__}")
        except APIResponseError as e:
            print(f"  API error: {e.code} — {e}")
            return
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
        print()

    _section("get_recent_deals(days=30)", lambda: get_recent_deals(30))
    _section("get_meetings(days=30)", lambda: get_meetings(days=30))
    _section("get_open_actions()", get_open_actions)
    _section("search_across('ul')", lambda: search_across("ul"))
    _section("get_objections()", get_objections)
    _section("get_objections(category='Budget')", lambda: get_objections(category="Budget"))
    _section("get_proof_points()", get_proof_points)
    _section("get_competitors()", get_competitors)
    _section("get_active_sprint()", get_active_sprint)

    # Exercise the cross-DB helpers with the most recent deal.
    recent = get_recent_deals(60)
    if recent:
        sample = recent[0]["title"]
        _section(f"get_meetings_for_deal({sample!r})",
                 lambda: get_meetings_for_deal(sample))
        print(f"— get_deal_context({sample!r}) —")
        try:
            ctx = get_deal_context(sample)
            if ctx:
                print(f"  deal: {ctx['deal']['title']}")
                print(f"  company: {(ctx['company'] or {}).get('title') or '(none)'}")
                print(f"  contacts: {len(ctx['contacts'])}")
                print(f"  meetings: {len(ctx['meetings'])}")
                print(f"  competitive_intel: {len(ctx['competitive_intel'])}")
            else:
                print("  (no deal found)")
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
        print()
    return 0


# ---------------------------------------------------------------------------
# Master CRM — Companies and Contacts
# ---------------------------------------------------------------------------
#
# These two databases are the single source of truth for all SDR work. Every
# lead, every company, every contact lives here regardless of product line or
# sprint. Relations (all dual_property unless noted):
#
#     Contacts.Company        ↔ Companies.Contacts
#     Contacts.Deals          ↔ Deals.Contacts
#     Deals."Company (linked)" ↔ Companies."Related to Deals (Company (linked))"
#     Meetings.Deal           ↔ Deals.Meetings
#     Companies."Competitive Intel" ↔ "Competitive Intel".Companies
#     Companies.Demos (legacy) ↔ Deals."Related to Companies (Demos)"
#
# Notion 2025 API: writes go through data_sources, not databases. The
# _resolve_data_source_id helper (defined above) caches the data-source id for
# each DB.

import time
import re

NOTION_RATE_LIMIT_DELAY = 0.35  # 3 req/s ceiling → pause 0.35s between writes

_COMPANY_SCHEMA = {
    "Name": "title",
    "Industry": "select",
    "Employee Count": "select",
    "Revenue": "rich_text",
    "Website": "url",
    "HQ Location": "rich_text",
    "CDP Score": "select",
    "ESG Report URL": "url",
    "Product Interest": "multi_select",
    "Source": "select",
    "Status": "select",
    "Notes": "rich_text",
}

_CONTACT_SCHEMA = {
    "Name": "title",
    "Email": "email",
    "Phone": "phone_number",
    "Mobile": "phone_number",
    "Title": "rich_text",
    "Department": "select",
    "LinkedIn": "url",
    "Role": "select",
    "Product Interest": "multi_select",
    "Campaigns": "multi_select",
    "Status": "select",
    "Last Contacted": "date",
    "Last Contact Method": "select",
    "Source": "select",
    "Time Zone": "select",
    "Area Code": "rich_text",
    "Notes": "rich_text",
}


def _build_property_value(ptype: str, value: Any) -> Optional[dict]:
    """Inverse of _extract_property: turn a plain Python value into the Notion
    payload for one property. Returns None to signal 'skip this property'
    (used when value is None or empty-string for a non-multi-select field).
    """
    if value is None:
        return None
    if ptype == "title":
        text = str(value).strip()
        if not text:
            return None
        return {"title": _rich_text(text)}
    if ptype == "rich_text":
        text = str(value).strip()
        if not text:
            return None
        return {"rich_text": _rich_text(text)}
    if ptype == "select":
        text = str(value).strip()
        if not text:
            return None
        return {"select": {"name": text}}
    if ptype == "multi_select":
        if isinstance(value, str):
            items = [v.strip() for v in value.split(",") if v.strip()]
        else:
            items = [str(v).strip() for v in value if str(v).strip()]
        return {"multi_select": [{"name": v} for v in items]}
    if ptype == "date":
        if isinstance(value, dict):
            return {"date": value}
        return {"date": {"start": str(value)}}
    if ptype == "url":
        text = str(value).strip()
        if not text:
            return None
        return {"url": text}
    if ptype == "email":
        text = str(value).strip()
        if not text:
            return None
        return {"email": text}
    if ptype == "phone_number":
        text = str(value).strip()
        if not text:
            return None
        return {"phone_number": text}
    if ptype == "checkbox":
        return {"checkbox": bool(value)}
    if ptype == "number":
        return {"number": float(value) if not isinstance(value, int) else int(value)}
    if ptype == "relation":
        ids = value if isinstance(value, list) else [value]
        return {"relation": [{"id": str(i)} for i in ids if i]}
    return None


def _build_props(schema: dict[str, str], data: dict) -> dict:
    """Map a flat dict (lowercase keys with _ or spaces) to Notion properties
    payload using the provided schema. Unknown keys are silently dropped.
    Relation fields must be passed with the property name as-is and a list of
    page ids as the value.
    """
    out: dict = {}
    for key, ptype in schema.items():
        # accept both "Product Interest" and "product_interest" / "product interest"
        val = data.get(key)
        if val is None:
            val = data.get(key.lower())
        if val is None:
            val = data.get(key.lower().replace(" ", "_"))
        if val is None:
            continue
        payload = _build_property_value(ptype, val)
        if payload is not None:
            out[key] = payload
    return out


def _fuzzy_title_match(pages: list[dict], needle: str) -> list[dict]:
    """Return entries whose title contains `needle` (case-insensitive), most-
    recently-edited first."""
    n = (needle or "").strip().lower()
    if not n:
        return []
    hits = [_page_to_dict(p) for p in pages if n in (_title_of(p) or "").lower()]
    hits.sort(key=lambda e: e.get("last_edited_time") or "", reverse=True)
    return hits


# --- Companies -------------------------------------------------------------

def get_companies(
    status: Optional[str] = None,
    product: Optional[str] = None,
    industry: Optional[str] = None,
) -> list[dict]:
    """Return Companies entries, optionally filtered (case-insensitive).
    `product` matches any value in Product Interest (multi-select).
    """
    pages = _query_all(
        _env("NOTION_COMPANIES_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if status:
        s = status.lower()
        out = [e for e in out if (e["properties"].get("Status") or "").lower() == s]
    if industry:
        i = industry.lower()
        out = [e for e in out if (e["properties"].get("Industry") or "").lower() == i]
    if product:
        p = product.lower()
        out = [
            e for e in out
            if any((x or "").lower() == p for x in (e["properties"].get("Product Interest") or []))
        ]
    return out


def get_company(name: str) -> Optional[dict]:
    """Fuzzy substring match on Company Name. Returns most recently edited
    match with body text included, or None."""
    if not (name or "").strip():
        return None
    pages = _query_all(_env("NOTION_COMPANIES_DB_ID"))
    matches = _fuzzy_title_match(pages, name)
    if not matches:
        return None
    top = matches[0]
    top["body"] = _get_page_body(top["id"])
    return top


def add_company(data: dict) -> dict:
    """Create a Companies entry. Required: `name` (or `Name`). All other keys
    match property names (case-insensitive; underscores or spaces OK). Returns
    the created page dict (via _page_to_dict)."""
    client = _get_client()
    data_source_id = _resolve_data_source_id(_env("NOTION_COMPANIES_DB_ID"))
    if not (data.get("Name") or data.get("name")):
        raise ValueError("add_company requires 'name'")
    props = _build_props(_COMPANY_SCHEMA, data)
    page = client.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties=props,
    )
    return _page_to_dict(page)


def update_company(page_id: str, data: dict) -> dict:
    """Update a Companies entry in place. Only the fields present in `data`
    are touched; everything else is left alone. Returns the updated page."""
    client = _get_client()
    props = _build_props(_COMPANY_SCHEMA, data)
    if not props:
        return _page_to_dict(client.pages.retrieve(page_id=page_id))
    page = client.pages.update(page_id=page_id, properties=props)
    return _page_to_dict(page)


# --- Contacts --------------------------------------------------------------

def _resolve_company_id(company_name: str, create_if_missing: bool = True,
                        _cache: Optional[dict] = None) -> Optional[str]:
    """Look up a Company page by name; optionally create a stub if absent.
    `_cache`, when supplied, is a {lower(name): page_id} dict that callers use
    to avoid re-querying Notion on every row of a bulk import.
    """
    name = (company_name or "").strip()
    if not name:
        return None
    key = name.lower()
    if _cache is not None and key in _cache:
        return _cache[key]
    # Exact-match-first (case-insensitive) then fuzzy
    pages = _query_all(_env("NOTION_COMPANIES_DB_ID"))
    for p in pages:
        t = (_title_of(p) or "").strip().lower()
        if t == key:
            if _cache is not None:
                _cache[key] = p["id"]
            return p["id"]
    fuzzy = _fuzzy_title_match(pages, name)
    if fuzzy:
        pid = fuzzy[0]["id"]
        if _cache is not None:
            _cache[key] = pid
        return pid
    if not create_if_missing:
        return None
    created = add_company({"name": name, "status": "New"})
    if _cache is not None:
        _cache[key] = created["id"]
    return created["id"]


def get_contacts(
    company: Optional[str] = None,
    role: Optional[str] = None,
    status: Optional[str] = None,
    campaign: Optional[str] = None,
    product: Optional[str] = None,
) -> list[dict]:
    """Return Contacts entries, optionally filtered (case-insensitive).
    `company` matches against the resolved Company relation's title.
    `campaign` and `product` are multi-select any-match.
    """
    pages = _query_all(
        _env("NOTION_CONTACTS_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if role:
        r = role.lower()
        out = [e for e in out if (e["properties"].get("Role") or "").lower() == r]
    if status:
        s = status.lower()
        out = [e for e in out if (e["properties"].get("Status") or "").lower() == s]
    if campaign:
        c = campaign.lower()
        out = [
            e for e in out
            if any((x or "").lower() == c for x in (e["properties"].get("Campaigns") or []))
        ]
    if product:
        p = product.lower()
        out = [
            e for e in out
            if any((x or "").lower() == p for x in (e["properties"].get("Product Interest") or []))
        ]
    if company:
        # Resolve the target company and match by relation id
        target = get_company(company)
        if target is None:
            return []
        target_id = target["id"]
        out = [e for e in out if target_id in (e["properties"].get("Company") or [])]
    return out


def get_contact(name: str) -> Optional[dict]:
    """Fuzzy substring match on Contact Name. Returns most recently edited
    match with body text included, or None."""
    if not (name or "").strip():
        return None
    pages = _query_all(_env("NOTION_CONTACTS_DB_ID"))
    matches = _fuzzy_title_match(pages, name)
    if not matches:
        return None
    top = matches[0]
    top["body"] = _get_page_body(top["id"])
    return top


def add_contact(data: dict, _company_cache: Optional[dict] = None) -> dict:
    """Create a Contacts entry. Required: `name`. Optional `company_name`
    (str) auto-resolves to the Company relation — creating a stub company if
    none exists. `_company_cache` is an optional caller-supplied dict used by
    bulk_import to avoid repeated Notion lookups; normal callers should leave
    it unset. Returns the created page dict."""
    client = _get_client()
    data_source_id = _resolve_data_source_id(_env("NOTION_CONTACTS_DB_ID"))
    if not (data.get("Name") or data.get("name")):
        raise ValueError("add_contact requires 'name'")
    props = _build_props(_CONTACT_SCHEMA, data)
    company_name = data.get("company_name") or data.get("Company") or data.get("company")
    if company_name:
        company_id = _resolve_company_id(
            company_name, create_if_missing=True, _cache=_company_cache
        )
        if company_id:
            props["Company"] = {"relation": [{"id": company_id}]}
    page = client.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties=props,
    )
    return _page_to_dict(page)


def update_contact(page_id: str, data: dict) -> dict:
    """Update a Contacts entry in place. Only the fields present in `data`
    are touched. Returns the updated page."""
    client = _get_client()
    props = _build_props(_CONTACT_SCHEMA, data)
    company_name = data.get("company_name") or data.get("Company") or data.get("company")
    if company_name:
        company_id = _resolve_company_id(company_name, create_if_missing=True)
        if company_id:
            props["Company"] = {"relation": [{"id": company_id}]}
    if not props:
        return _page_to_dict(client.pages.retrieve(page_id=page_id))
    page = client.pages.update(page_id=page_id, properties=props)
    return _page_to_dict(page)


# --- Query helpers for outreach scripts ------------------------------------

def _strip_area_code(phone: str) -> str:
    """Pull a 3-digit US area code from any phone string. Returns '' if none."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits[:3] if len(digits) >= 10 else ""


def get_contacts_by_area_code(area_code: str) -> list[dict]:
    """Return contacts whose Area Code property equals `area_code`, falling
    back to parsing the Phone property. For call-batching by geography."""
    target = (area_code or "").strip()
    if not target:
        return []
    out: list[dict] = []
    for entry in get_contacts():
        stored = (entry["properties"].get("Area Code") or "").strip()
        if stored == target:
            out.append(entry)
            continue
        parsed = _strip_area_code(entry["properties"].get("Phone") or "")
        if parsed == target:
            out.append(entry)
    return out


def get_contacts_by_campaign(campaign_name: str) -> list[dict]:
    """Return contacts whose Campaigns multi-select contains `campaign_name`."""
    return get_contacts(campaign=campaign_name)


def get_contacts_by_timezone(tz: str) -> list[dict]:
    """Return contacts whose Time Zone select matches `tz` (case-insensitive)."""
    target = (tz or "").strip().lower()
    if not target:
        return []
    return [
        e for e in get_contacts()
        if (e["properties"].get("Time Zone") or "").lower() == target
    ]


def update_contact_status(
    name: str,
    status: str,
    method: Optional[str] = None,
) -> dict:
    """Quick status update after a touch — auto-stamps Last Contacted = today
    and sets Last Contact Method if provided. `name` is fuzzy-matched. Raises
    ValueError if no contact found."""
    contact = get_contact(name)
    if contact is None:
        raise ValueError(f"No contact found matching {name!r}")
    data: dict = {
        "Status": status,
        "Last Contacted": datetime.now(timezone.utc).date().isoformat(),
    }
    if method:
        data["Last Contact Method"] = method
    return update_contact(contact["id"], data)


# --- Bulk import -----------------------------------------------------------
#
# Column mappings live at the top of each function so different CSV formats
# (Salesforce export, ZoomInfo export, your own research sheet) can be
# supported by editing the mapping dict rather than branching the code.

# Default mapping: source-column-name (lowercase, stripped) → property name.
# Callers can extend these by passing a `column_map` arg.
DEFAULT_COMPANY_COLUMN_MAP = {
    "name": "Name",
    "company": "Name",
    "company name": "Name",
    "account": "Name",
    "account name": "Name",
    "industry": "Industry",
    "employees": "Employee Count",
    "employee count": "Employee Count",
    "revenue": "Revenue",
    "annual revenue": "Revenue",
    "website": "Website",
    "domain": "Website",
    "hq": "HQ Location",
    "hq location": "HQ Location",
    "headquarters": "HQ Location",
    "city": "HQ Location",
    "cdp score": "CDP Score",
    "cdp grade": "CDP Score",
    "esg report": "ESG Report URL",
    "esg report url": "ESG Report URL",
    "product interest": "Product Interest",
    "source": "Source",
    "lead source": "Source",
    "status": "Status",
    "notes": "Notes",
    "description": "Notes",
}

DEFAULT_CONTACT_COLUMN_MAP = {
    "name": "Name",
    "full name": "Name",
    "contact": "Name",
    "email": "Email",
    "email address": "Email",
    "phone": "Phone",
    "phone number": "Phone",
    "work phone": "Phone",
    "mobile": "Mobile",
    "cell": "Mobile",
    "mobile phone": "Mobile",
    "title": "Title",
    "job title": "Title",
    "department": "Department",
    "linkedin": "LinkedIn",
    "linkedin url": "LinkedIn",
    "role": "Role",
    "product interest": "Product Interest",
    "campaigns": "Campaigns",
    "campaign": "Campaigns",
    "status": "Status",
    "last contacted": "Last Contacted",
    "last contact": "Last Contacted",
    "last contact method": "Last Contact Method",
    "source": "Source",
    "lead source": "Source",
    "time zone": "Time Zone",
    "timezone": "Time Zone",
    "area code": "Area Code",
    "notes": "Notes",
    "description": "Notes",
    # relation
    "company": "company_name",
    "company name": "company_name",
    "account": "company_name",
    "account name": "company_name",
}


def _read_tabular(path: str):
    """Load CSV or Excel into a list of dicts, lowercase-stripped keys."""
    import pandas as pd
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"{path} not found")
    if p.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(p, dtype=str)
    else:
        df = pd.read_csv(p, dtype=str)
    df = df.fillna("")
    rows: list[dict] = []
    for _, row in df.iterrows():
        out: dict = {}
        for col, val in row.items():
            k = str(col).strip().lower()
            out[k] = (val or "").strip() if isinstance(val, str) else val
        rows.append(out)
    return rows


def _apply_column_map(row: dict, column_map: dict) -> dict:
    """Translate a source-row dict to a Notion-shaped dict via column_map."""
    out: dict = {}
    for src_col, val in row.items():
        dest = column_map.get(src_col)
        if not dest:
            continue
        if val == "" or val is None:
            continue
        out[dest] = val
    return out


def bulk_import_companies(
    csv_path: str,
    column_map: Optional[dict] = None,
    dry_run: bool = False,
) -> dict:
    """Bulk-create Companies from a CSV/Excel file.

    Dupes (case-insensitive Name match against what's already in Notion) are
    skipped. Returns {"created": N, "skipped": N, "errors": [...]}. Prints
    progress every 50 records. Throttles to ~3 req/s.

    `column_map` is merged over `DEFAULT_COMPANY_COLUMN_MAP`. Edit the default
    map (or pass an override) when a new export format arrives.
    """
    cmap = dict(DEFAULT_COMPANY_COLUMN_MAP)
    if column_map:
        cmap.update({k.lower(): v for k, v in column_map.items()})

    rows = _read_tabular(csv_path)
    existing = {
        (_title_of(p) or "").strip().lower()
        for p in _query_all(_env("NOTION_COMPANIES_DB_ID"))
    }

    created = 0
    skipped = 0
    errors: list[str] = []
    total = len(rows)
    print(f"[bulk_import_companies] {total} rows from {csv_path}")

    for i, row in enumerate(rows, start=1):
        mapped = _apply_column_map(row, cmap)
        name = (mapped.get("Name") or "").strip()
        if not name:
            skipped += 1
            errors.append(f"row {i}: no Name")
            continue
        key = name.lower()
        if key in existing:
            skipped += 1
            if i % 50 == 0:
                print(f"  [{i}/{total}] created={created} skipped={skipped}")
            continue
        if dry_run:
            created += 1
            existing.add(key)
        else:
            try:
                add_company(mapped)
                existing.add(key)
                created += 1
                time.sleep(NOTION_RATE_LIMIT_DELAY)
            except Exception as e:
                errors.append(f"row {i} ({name}): {type(e).__name__}: {e}")
                skipped += 1
        if i % 50 == 0:
            print(f"  [{i}/{total}] created={created} skipped={skipped}")

    print(f"[bulk_import_companies] done — created={created} skipped={skipped} errors={len(errors)}")
    return {"created": created, "skipped": skipped, "errors": errors}


def bulk_import_contacts(
    csv_path: str,
    column_map: Optional[dict] = None,
    dry_run: bool = False,
) -> dict:
    """Bulk-create Contacts from a CSV/Excel file.

    - Auto-creates Company entries for any company name not yet in Notion.
    - Skips dupes by (Name + Company) case-insensitive match against existing
      contacts. If a contact has no Company, dedupe is by Name alone.
    - Prints progress every 50 records. Throttles to ~3 req/s.

    `column_map` is merged over `DEFAULT_CONTACT_COLUMN_MAP`. Edit the default
    map (or pass an override) when a new export format arrives.
    """
    cmap = dict(DEFAULT_CONTACT_COLUMN_MAP)
    if column_map:
        cmap.update({k.lower(): v for k, v in column_map.items()})

    rows = _read_tabular(csv_path)

    # Snapshot existing contacts for dedupe and companies for relation cache
    company_cache: dict[str, str] = {}
    for p in _query_all(_env("NOTION_COMPANIES_DB_ID")):
        t = (_title_of(p) or "").strip().lower()
        if t:
            company_cache[t] = p["id"]

    existing_contacts: set[tuple] = set()
    # Reverse lookup: company page_id → lowercased company name
    id_to_company = {v: k for k, v in company_cache.items()}
    for p in _query_all(_env("NOTION_CONTACTS_DB_ID")):
        entry = _page_to_dict(p)
        cname_key = ""
        rel_ids = entry["properties"].get("Company") or []
        if rel_ids:
            cname_key = id_to_company.get(rel_ids[0], "")
        existing_contacts.add(((entry["title"] or "").strip().lower(), cname_key))

    created = 0
    skipped = 0
    errors: list[str] = []
    total = len(rows)
    print(f"[bulk_import_contacts] {total} rows from {csv_path}")

    for i, row in enumerate(rows, start=1):
        mapped = _apply_column_map(row, cmap)
        name = (mapped.get("Name") or "").strip()
        if not name:
            skipped += 1
            errors.append(f"row {i}: no Name")
            continue
        company_name = (mapped.get("company_name") or "").strip()
        dedupe_key = (name.lower(), company_name.lower())
        if dedupe_key in existing_contacts:
            skipped += 1
            if i % 50 == 0:
                print(f"  [{i}/{total}] created={created} skipped={skipped}")
            continue

        if dry_run:
            created += 1
            existing_contacts.add(dedupe_key)
        else:
            try:
                # If we'll need to create a new company, that's an extra API
                # call — factor it into the throttle implicitly via the two
                # sleeps below.
                need_new_company = bool(company_name) and company_name.lower() not in company_cache
                add_contact(mapped, _company_cache=company_cache)
                existing_contacts.add(dedupe_key)
                created += 1
                time.sleep(NOTION_RATE_LIMIT_DELAY)
                if need_new_company:
                    time.sleep(NOTION_RATE_LIMIT_DELAY)
            except Exception as e:
                errors.append(f"row {i} ({name}): {type(e).__name__}: {e}")
                skipped += 1
        if i % 50 == 0:
            print(f"  [{i}/{total}] created={created} skipped={skipped}")

    print(f"[bulk_import_contacts] done — created={created} skipped={skipped} errors={len(errors)}")
    return {"created": created, "skipped": skipped, "errors": errors}


def _cli_test_crm() -> int:
    """Round-trip smoke test for the Companies + Contacts CRM. Creates a pair,
    exercises the query helpers, does an update, and leaves the test rows in
    place (prefixed 'TEST — ') so you can eyeball them in Notion and delete
    manually."""
    print("=" * 60)
    print("core.notion — CRM round-trip smoke test")
    print("=" * 60)
    for name in ("NOTION_COMPANIES_DB_ID", "NOTION_CONTACTS_DB_ID"):
        val = os.getenv(name) or ""
        print(f"  {name}: {'set' if val else 'MISSING'}")
    if not os.getenv("NOTION_COMPANIES_DB_ID") or not os.getenv("NOTION_CONTACTS_DB_ID"):
        return 1
    print()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    co_name = f"TEST — Acme Test Co {stamp}"
    ct_name = f"TEST — Alex Tester {stamp}"

    try:
        print(f"  add_company({co_name!r})")
        co = add_company({
            "name": co_name,
            "industry": "Manufacturing",
            "employee_count": "501-2000",
            "revenue": "$500M",
            "website": "https://example.com",
            "hq_location": "Chicago, IL",
            "cdp_score": "B",
            "product_interest": ["Product Stewardship"],
            "source": "Cold Research",
            "status": "Researching",
            "notes": "Created by CLI test",
        })
        print(f"    → {co['id']}")

        print(f"  add_contact({ct_name!r}) with company_name")
        ct = add_contact({
            "name": ct_name,
            "email": "alex@example.com",
            "phone": "+13125551212",
            "title": "Head of Sustainability",
            "department": "Sustainability/ESG",
            "role": "Decision Maker",
            "product_interest": ["Product Stewardship"],
            "campaigns": ["Example Campaign 2026"],
            "status": "New",
            "time_zone": "CT",
            "area_code": "312",
            "source": "Cold Research",
            "company_name": co_name,
        })
        print(f"    → {ct['id']}")

        print("  get_company(co_name)")
        g = get_company(co_name)
        print(f"    → {'found' if g else 'missing'}  body_len={len((g or {}).get('body',''))}")

        print("  get_contact(ct_name)")
        g = get_contact(ct_name)
        print(f"    → {'found' if g else 'missing'}")

        print("  get_contacts(company=co_name)")
        lst = get_contacts(company=co_name)
        print(f"    → {len(lst)} contact(s) linked")

        print("  get_contacts_by_area_code('312')")
        lst = get_contacts_by_area_code("312")
        print(f"    → {len(lst)} contact(s)")

        print("  get_contacts_by_campaign('Example Campaign 2026')")
        lst = get_contacts_by_campaign("Example Campaign 2026")
        print(f"    → {len(lst)} contact(s)")

        print("  get_contacts_by_timezone('CT')")
        lst = get_contacts_by_timezone("CT")
        print(f"    → {len(lst)} contact(s)")

        print("  update_contact_status(ct_name, 'Contacted', method='Email')")
        updated = update_contact_status(ct_name, "Contacted", method="Email")
        print(f"    → status={updated['properties'].get('Status')} "
              f"last={updated['properties'].get('Last Contacted')}")

        print("  update_company(co_id, {'status': 'Active'})")
        updated = update_company(co["id"], {"status": "Active"})
        print(f"    → status={updated['properties'].get('Status')}")

        print()
        print("All checks passed. Leaving TEST rows in Notion for visual inspection.")
        print(f"  Company page: {co.get('url')}")
        print(f"  Contact page: {ct.get('url')}")
        return 0
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1


# ---------------------------------------------------------------------------
# Tasks — mirror of config/tasks.json in Notion
# ---------------------------------------------------------------------------
#
# tasks.json is the source of truth; Notion is the read layer Hugh can see on
# his phone / outside Claude Code. Writes happen in both places; sync is
# one-way JSON → Notion (idempotent, Lead ID is the join key).

_TASK_SCHEMA = {
    "Name": "title",
    "Lead ID": "rich_text",
    "Status": "select",
    "Due Date": "date",
    "Priority": "select",
    "Confidence": "number",
    "Channel": "select",
    "Source": "rich_text",
    "Reason": "rich_text",
    "Created At": "date",
}


def _task_record_to_props(record: dict) -> dict:
    """Translate a tasks.json record into a _build_props-ready dict."""
    return {
        "Name": record.get("title") or "(untitled task)",
        "Lead ID": record.get("lead_id") or "",
        "Status": record.get("status") or "open",
        "Due Date": (record.get("due_at") or "")[:10] or None,
        "Priority": record.get("priority") or None,
        "Confidence": record.get("confidence"),
        "Channel": record.get("channel") or None,
        "Source": record.get("source") or record.get("reason") or None,
        "Reason": record.get("reason") or None,
        "Created At": (record.get("created_at") or "")[:10] or None,
    }


def get_task(lead_id: str) -> Optional[dict]:
    """Look up a Tasks page by Lead ID. Returns None if no match."""
    key = (lead_id or "").strip()
    if not key:
        return None
    pages = _query_all(
        _env("NOTION_TASKS_DB_ID"),
        filter={"property": "Lead ID", "rich_text": {"equals": key}},
    )
    if not pages:
        return None
    return _page_to_dict(pages[0])


def get_tasks(status: Optional[str] = None) -> list[dict]:
    """Return Tasks entries, newest first. Optional case-insensitive Status filter."""
    pages = _query_all(
        _env("NOTION_TASKS_DB_ID"),
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}],
    )
    out = [_page_to_dict(p) for p in pages]
    if status:
        s = status.lower()
        out = [e for e in out if (e["properties"].get("Status") or "").lower() == s]
    return out


def add_task(record: dict) -> dict:
    """Create a Tasks page from a tasks.json-shaped record. Requires lead_id and title."""
    if not record.get("lead_id"):
        raise ValueError("add_task requires 'lead_id'")
    if not record.get("title"):
        raise ValueError("add_task requires 'title'")
    client = _get_client()
    data_source_id = _resolve_data_source_id(_env("NOTION_TASKS_DB_ID"))
    props = _build_props(_TASK_SCHEMA, _task_record_to_props(record))
    page = client.pages.create(
        parent={"type": "data_source_id", "data_source_id": data_source_id},
        properties=props,
    )
    return _page_to_dict(page)


def update_task(page_id: str, record: dict) -> dict:
    """Update a Tasks page in place from a partial tasks.json-shaped record."""
    client = _get_client()
    props = _build_props(_TASK_SCHEMA, _task_record_to_props(record))
    if not props:
        return _page_to_dict(client.pages.retrieve(page_id=page_id))
    page = client.pages.update(page_id=page_id, properties=props)
    return _page_to_dict(page)


def close_task_in_notion(lead_id: str, reason: Optional[str] = None) -> Optional[dict]:
    """Mark the matching Tasks page Status = closed. No-op if not found."""
    existing = get_task(lead_id)
    if not existing:
        return None
    patch = {"lead_id": lead_id, "status": "closed"}
    if reason:
        patch["reason"] = reason
    return update_task(existing["id"], patch)


def sync_tasks_from_json(
    tasks_json_path: Optional[str] = None,
    dry_run: bool = False,
) -> dict:
    """One-way sync: tasks.json → Notion Tasks DB.

    Idempotent. For each record in tasks.json:
      - if no Notion page with matching Lead ID: create
      - if one exists: update status / due / priority / confidence / reason
    Orphan Notion pages (Lead ID not in JSON) are left alone — Notion may also
    host manually-added tasks. Returns counts and errors.
    """
    import json as _json
    path = tasks_json_path or str(_PROJECT_ROOT / "config" / "tasks.json")
    try:
        tasks = _json.loads(Path(path).read_text())
    except Exception as e:
        print(f"[notion] sync_tasks: could not read {path} — {e}", file=sys.stderr)
        return {"created": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
    if not isinstance(tasks, dict):
        return {"created": 0, "updated": 0, "skipped": 0, "errors": ["tasks.json not a dict"]}

    # Index existing Notion pages by Lead ID
    try:
        pages = _query_all(_env("NOTION_TASKS_DB_ID"))
    except Exception as e:
        print(f"[notion] sync_tasks: Notion query failed — {e}", file=sys.stderr)
        return {"created": 0, "updated": 0, "skipped": 0, "errors": [str(e)]}
    by_lead_id: dict[str, str] = {}
    for p in pages:
        entry = _page_to_dict(p)
        lid = (entry["properties"].get("Lead ID") or "").strip()
        if lid:
            by_lead_id[lid] = entry["id"]

    created = updated = skipped = 0
    errors: list[str] = []
    total = len(tasks)
    print(f"[notion] sync_tasks: {total} records in {path}")
    for i, (lead_id, record) in enumerate(tasks.items(), start=1):
        if not isinstance(record, dict):
            skipped += 1
            continue
        record = {**record, "lead_id": record.get("lead_id") or lead_id}
        if dry_run:
            if lead_id in by_lead_id:
                updated += 1
            else:
                created += 1
            continue
        try:
            if lead_id in by_lead_id:
                update_task(by_lead_id[lead_id], record)
                updated += 1
            else:
                add_task(record)
                created += 1
            time.sleep(NOTION_RATE_LIMIT_DELAY)
        except Exception as e:
            errors.append(f"{lead_id}: {type(e).__name__}: {e}")
            skipped += 1
        if i % 25 == 0:
            print(f"  [{i}/{total}] created={created} updated={updated} skipped={skipped}")
    print(f"[notion] sync_tasks: done — created={created} updated={updated} "
          f"skipped={skipped} errors={len(errors)}")
    return {"created": created, "updated": updated, "skipped": skipped, "errors": errors}


def create_tasks_database(parent_page_id: str, title: str = "Tasks") -> dict:
    """Bootstrap a Tasks database under `parent_page_id`. Returns the created
    database (raw). Print the `id` field and paste into .env as
    NOTION_TASKS_DB_ID. The integration must already be shared with the
    parent page.

    Schema matches _TASK_SCHEMA so sync_tasks_from_json works out of the box.
    """
    client = _get_client()
    parent_id = parent_page_id.replace("-", "").strip()
    properties = {
        "Name": {"title": {}},
        "Lead ID": {"rich_text": {}},
        "Status": {
            "select": {
                "options": [
                    {"name": "open", "color": "yellow"},
                    {"name": "pending", "color": "yellow"},
                    {"name": "in_progress", "color": "blue"},
                    {"name": "done", "color": "green"},
                    {"name": "closed", "color": "gray"},
                ]
            }
        },
        "Due Date": {"date": {}},
        "Priority": {
            "select": {
                "options": [
                    {"name": "high", "color": "red"},
                    {"name": "medium", "color": "orange"},
                    {"name": "low", "color": "gray"},
                ]
            }
        },
        "Confidence": {"number": {"format": "percent"}},
        "Channel": {
            "select": {
                "options": [
                    {"name": "email", "color": "blue"},
                    {"name": "call", "color": "green"},
                    {"name": "linkedin", "color": "purple"},
                    {"name": "whatsapp", "color": "green"},
                    {"name": "text", "color": "green"},
                    {"name": "crm", "color": "orange"},
                ]
            }
        },
        "Source": {"rich_text": {}},
        "Reason": {"rich_text": {}},
        "Created At": {"date": {}},
    }
    return client.databases.create(
        parent={"type": "page_id", "page_id": parent_id},
        title=[{"type": "text", "text": {"content": title}}],
        properties=properties,
    )


if __name__ == "__main__":
    if "--test" in sys.argv:
        raise SystemExit(_cli_test())
    if "--test-crm" in sys.argv:
        raise SystemExit(_cli_test_crm())
    print("Usage: python -m core.notion --test | --test-crm")
