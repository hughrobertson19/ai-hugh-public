#!/usr/bin/env python3
"""process_call.py — Convert discovery call notes into a task entry in config/tasks.json.

Reads call notes (default: call_notes.md), loads existing tasks for schema/matching context,
calls Claude to extract a structured task + multi-threading/handoff flags, prints both for
review, then appends or overwrites on confirmation.

Stdlib + anthropic SDK only. Run from project root.

Usage:
    python3 process_call.py
    python3 process_call.py --file path/to/notes.md
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import anthropic


ROOT = Path(__file__).resolve().parent
TASKS_FILE = ROOT / "config" / "tasks.json"
ROUTING_MAP_FILE = ROOT / "intel" / "example_campaign" / "sellers_routing.json"
MEMORY_FILE = ROOT / "data" / "users" / "hugh" / "memory.json"
DEFAULT_CALL_NOTES = ROOT / "call_notes.md"

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 2048
TEMPERATURE = 0.2

REQUIRED_FIELDS = {
    "lead_id", "title", "capability", "action_type", "due_at",
    "created_at", "status", "attempts", "channel", "focus_type",
    "confidence", "reason",
}
ALLOWED_CAPABILITIES = {"follow_up", "discovery", "handoff", "outbound"}
ALLOWED_CHANNELS = {"email", "call", "linkedin", "sms", "crm"}


# ── helpers ──────────────────────────────────────────────────────────────────

def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


# ── input loaders ────────────────────────────────────────────────────────────

def read_call_notes(path: Path) -> str:
    if not path.exists():
        die(f"Call notes file not found: {path}")
    text = path.read_text().strip()
    if not text:
        die(f"Call notes file is empty: {path}")
    return text


def load_existing_tasks() -> dict:
    if not TASKS_FILE.exists():
        die(f"{TASKS_FILE} not found — this tool writes into an existing tasks.json.")
    try:
        data = json.loads(TASKS_FILE.read_text())
    except json.JSONDecodeError as e:
        die(f"{TASKS_FILE} is not valid JSON: {e}")
    if not isinstance(data, dict):
        die(f"{TASKS_FILE} top-level must be a JSON object, got {type(data).__name__}.")
    return data


def load_routing_map() -> str | None:
    if not ROUTING_MAP_FILE.exists():
        return None
    try:
        return json.dumps(json.loads(ROUTING_MAP_FILE.read_text()), indent=2)
    except (IOError, json.JSONDecodeError):
        return None


def load_hugh_memory() -> dict | None:
    """Return parsed memory.json if readable as plain JSON; otherwise None.
    memory.json may be encrypted (ENC: prefix) by chat_server.py — skip in that
    case rather than guess a decryption key."""
    if not MEMORY_FILE.exists():
        return None
    try:
        raw = MEMORY_FILE.read_text()
    except IOError:
        return None
    if raw.startswith("ENC:"):
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


# ── prompt builders ──────────────────────────────────────────────────────────

def build_system_prompt() -> str:
    current = now_iso()
    return f"""You are a sales ops agent for Hugh Robertson (an SDR). Given raw discovery call notes, extract a single actionable task and Hugh-facing context.

Return STRICT JSON with this exact shape and no extra keys:
{{
  "task": {{
    "lead_id": "<snake_case slug, lowercase, ascii; match an existing tasks.json key if the account already exists, else propose a new one>",
    "title": "<imperative next action, <=80 chars>",
    "capability": "follow_up" | "discovery" | "handoff" | "outbound",
    "action_type": "<must equal capability>",
    "due_at": "<ISO-8601 naive datetime, e.g. 2026-04-21T10:00:00>",
    "created_at": "{current}",
    "status": "open",
    "attempts": 1,
    "channel": "email" | "call" | "linkedin" | "sms" | "crm",
    "focus_type": "project",
    "confidence": <float between 0.0 and 1.0>,
    "reason": "<2-3 sentences matching the voice of the style examples provided. Include: what happened on the call, specific dollar values / dates / product names mentioned, and what the next action hinges on. Terse, factual, no pleasantries.>",
    "country": "<optional country name if clearly stated in notes; omit the key entirely if unknown>"
  }},
  "notes_for_hugh": "<plain text, 3-6 short bullet lines, each prefixed with '- '. Include: (a) multi-threading gaps — contacts named in the call who weren't engaged yet; (b) handoff flags — if the routing map is provided and the lead's HQ location / product area maps to a specific seller, name them; (c) risks or blockers worth flagging.>"
}}

RULES:
- `capability`: "follow_up" when picking up a prior touch; "discovery" when a new discovery call is the next action; "handoff" when the next action is to route to a named seller; "outbound" when initiating fresh outreach.
- `action_type` must equal `capability` (they are duplicated in tasks.json by convention).
- `channel`: pick based on the stated or implied next step. Default to "email" if ambiguous.
- `due_at` must be strictly after {current}, must be a weekday (Monday through Friday), and defaults to 3-5 business days out unless the notes specify an explicit date. Never propose Saturday or Sunday. If the natural due date falls on a weekend, use the next Monday.
- `confidence` reflects the likelihood the deal moves forward, NOT your confidence in extraction.
- `lead_id`: match an existing slug exactly if the account is already tracked; otherwise invent a new lowercase snake_case slug derived from company or primary contact.
- Return ONLY the JSON object. No markdown fences, no commentary, no trailing prose."""


def build_user_prompt(
    call_notes: str,
    existing: dict,
    routing: str | None,
    memory: dict | None,
) -> str:
    # Compact existing tasks to lead_id: title — reason (truncated)
    summary_rows = []
    style_samples = []
    for lid, t in existing.items():
        if not isinstance(t, dict):
            continue
        title = t.get("title", "")
        reason = t.get("reason", "")
        summary_rows.append(f"  {lid}: {title} — {reason[:160]}")
        if reason and len(style_samples) < 3:
            style_samples.append(f"- {reason}")

    existing_summary = "\n".join(summary_rows) if summary_rows else "(none)"
    style_block = "\n".join(style_samples) if style_samples else "(no existing reasons to sample)"

    parts = [
        "=== CALL NOTES ===",
        call_notes,
        "",
        "=== EXISTING TASKS (for account matching — use an existing lead_id when the account is already tracked) ===",
        existing_summary,
        "",
        "=== REASON-FIELD STYLE ANCHORS (match this voice exactly) ===",
        style_block,
    ]

    if routing:
        parts += ["", "=== ROUTING MAP (for handoff flags) ===", routing[:6000]]
    else:
        parts += ["", "=== ROUTING MAP ===", "(not available — skip handoff seller attribution unless the call notes explicitly name a seller)"]

    if memory:
        prefs = memory.get("preferences") or {}
        mem_view = {
            "role": prefs.get("role"),
            "products": prefs.get("products"),
            "priorities": prefs.get("priorities"),
            "timezone": prefs.get("timezone"),
        }
        parts += ["", "=== HUGH'S CONTEXT ===", json.dumps(mem_view, indent=2)]

    return "\n".join(parts)


# ── API call + parsing ───────────────────────────────────────────────────────

def call_claude(system: str, user: str) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        die("ANTHROPIC_API_KEY is not set in the environment.")
    client = anthropic.Anthropic(api_key=key)
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
    except anthropic.APIError as e:
        die(f"Anthropic API error ({type(e).__name__}): {e}")
    if not resp.content or not hasattr(resp.content[0], "text"):
        die("Anthropic response had no text content.")
    return resp.content[0].text.strip()


def parse_response(raw: str) -> tuple[dict, str]:
    txt = raw.strip()
    # Pragmatic: strip ``` / ```json fences if the model wrapped them
    if txt.startswith("```"):
        txt = txt.strip("`")
        if txt.lower().startswith("json"):
            txt = txt[4:].lstrip()
        if txt.endswith("```"):
            txt = txt[:-3].rstrip()
    try:
        data = json.loads(txt)
    except json.JSONDecodeError as e:
        die(f"Model response is not valid JSON: {e}\n--- raw response ---\n{raw}")
    if not isinstance(data, dict):
        die("Model response is not a JSON object at the top level.")

    task = data.get("task")
    if not isinstance(task, dict):
        die("Response missing a 'task' object.")

    notes = data.get("notes_for_hugh", "")
    if not isinstance(notes, str):
        notes = ""

    validate_task(task)
    return task, notes


def validate_task(task: dict) -> None:
    missing = REQUIRED_FIELDS - set(task.keys())
    if missing:
        die(f"Task missing required fields: {sorted(missing)}")

    if task["capability"] not in ALLOWED_CAPABILITIES:
        die(f"capability must be one of {sorted(ALLOWED_CAPABILITIES)}, got {task['capability']!r}")
    if task["channel"] not in ALLOWED_CHANNELS:
        die(f"channel must be one of {sorted(ALLOWED_CHANNELS)}, got {task['channel']!r}")
    if task["status"] != "open":
        die(f"status must be 'open', got {task['status']!r}")
    if task["action_type"] != task["capability"]:
        die(f"action_type must equal capability: {task['action_type']!r} vs {task['capability']!r}")
    if task["focus_type"] != "project":
        die(f"focus_type must be 'project', got {task['focus_type']!r}")
    if task["attempts"] != 1:
        die(f"attempts must be 1 on a new task, got {task['attempts']!r}")

    for f in ("due_at", "created_at"):
        try:
            datetime.fromisoformat(task[f])
        except (ValueError, TypeError) as e:
            die(f"{f} is not parseable as ISO-8601: {task[f]!r} ({e})")

    due_dt = datetime.fromisoformat(task["due_at"])
    if due_dt.weekday() >= 5:
        day_name = ("Saturday", "Sunday")[due_dt.weekday() - 5]
        die(
            f"due_at {task['due_at']} falls on {day_name}. "
            f"due_at must be a weekday (Mon-Fri). "
            f"Re-run process_call.py to get a new proposal."
        )

    conf = task.get("confidence")
    if not isinstance(conf, (int, float)) or not (0.0 <= float(conf) <= 1.0):
        die(f"confidence must be a float between 0.0 and 1.0, got {conf!r}")

    lead_id = task.get("lead_id")
    if not isinstance(lead_id, str) or not lead_id.strip():
        die(f"lead_id must be a non-empty string, got {lead_id!r}")


# ── presentation + persistence ───────────────────────────────────────────────

def confirm(prompt: str) -> bool:
    try:
        ans = input(prompt).strip().lower()
    except EOFError:
        return False
    return ans in ("y", "yes")


def save_task(task: dict, existing: dict) -> None:
    lead_id = task["lead_id"]
    if lead_id in existing:
        print(f"\n!! lead_id '{lead_id}' already exists.")
        print(f"   existing title: {existing[lead_id].get('title', '(none)')}")
        print(f"   existing due_at: {existing[lead_id].get('due_at', '(none)')}")
        if not confirm(f"Overwrite existing task for {lead_id}? [y/N]: "):
            print("Aborted — nothing written.")
            return
    existing[lead_id] = task
    TASKS_FILE.write_text(json.dumps(existing, indent=2) + "\n")
    print(f"Saved to {TASKS_FILE} ({len(existing)} total tasks).")


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert discovery call notes into a config/tasks.json entry."
    )
    parser.add_argument(
        "--file",
        default=str(DEFAULT_CALL_NOTES),
        help="path to call notes file (default: call_notes.md at project root)",
    )
    args = parser.parse_args()

    notes_path = Path(args.file).expanduser().resolve()
    call_notes = read_call_notes(notes_path)
    existing = load_existing_tasks()
    routing = load_routing_map()
    memory = load_hugh_memory()

    system = build_system_prompt()
    user = build_user_prompt(call_notes, existing, routing, memory)

    print(f"Calling {MODEL} ...")
    raw = call_claude(system, user)
    task, notes_for_hugh = parse_response(raw)

    print("\n" + "=" * 56)
    print("PROPOSED TASK ENTRY")
    print("=" * 56)
    print(json.dumps({task["lead_id"]: task}, indent=2))
    print()

    if notes_for_hugh.strip():
        print("=" * 56)
        print("NOTES FOR HUGH (multi-threading + handoffs)")
        print("=" * 56)
        print(notes_for_hugh.strip())
        print()

    if not confirm("Write to config/tasks.json? [y/N]: "):
        print("Aborted — nothing written.")
        return

    save_task(task, existing)


if __name__ == "__main__":
    main()
