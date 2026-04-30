from typing import Optional, Dict
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo
import json
from pathlib import Path

TASKS_FILE = Path("tasks.json")
DECISIONS_FILE = Path("decision_memory.json")

# WhatsApp country tiers based on business adoption research.
# Tier 1: WhatsApp is the default business messaging channel — prefer over SMS.
# Tier 2: WhatsApp works as follow-up but not first contact — use after email/LinkedIn.
# Tier 3 (unlisted): stick with SMS/email/call.
WHATSAPP_TIER1_COUNTRIES = {
    # Latin America
    "brazil", "mexico", "colombia", "argentina", "chile", "peru",
    # India
    "india",
    # Middle East & North Africa
    "uae", "united arab emirates", "saudi arabia", "egypt", "turkey", "jordan",
    "qatar", "bahrain", "kuwait", "oman",
    # Sub-Saharan Africa
    "nigeria", "kenya", "south africa", "ghana",
    # Southeast Asia
    "indonesia", "malaysia", "thailand", "philippines",
}
WHATSAPP_TIER2_COUNTRIES = {
    # Western Europe — WhatsApp as warm follow-up, not cold outreach
    "germany", "spain", "italy", "netherlands", "uk", "united kingdom",
    "portugal", "austria", "switzerland", "belgium", "ireland",
}

def get_whatsapp_tier(country: str) -> int:
    """Return WhatsApp business adoption tier for a country.
    1 = default to WhatsApp, 2 = use as follow-up, 3 = don't use.
    """
    if not country:
        return 3
    normalized = country.strip().lower()
    if normalized in WHATSAPP_TIER1_COUNTRIES:
        return 1
    if normalized in WHATSAPP_TIER2_COUNTRIES:
        return 2
    return 3

def recommend_messaging_channel(task: dict) -> str:
    """Pick WhatsApp or text/SMS based on the lead's country.
    For Tier 1 countries, always prefer WhatsApp over text.
    For Tier 2 countries, prefer WhatsApp on follow-ups (attempts > 0).
    For Tier 3 (US/Canada/etc), stick with text.
    """
    country = task.get("country", "")
    tier = get_whatsapp_tier(country)
    current_channel = task.get("channel", "text")
    attempts = task.get("attempts", 0)

    if tier == 1:
        return "whatsapp"
    if tier == 2 and attempts > 0:
        return "whatsapp"
    # Tier 3 or Tier 2 first touch — keep original channel
    if current_channel == "whatsapp":
        return "whatsapp"  # respect manual override
    return current_channel

PREFERENCES_FILE = Path("preferences.json")

# Integration mode: "dry_run" (default) or "live"
def get_integration_mode() -> str:
    prefs = load_preferences()
    return prefs.get("integration_mode", "dry_run")


def load_preferences() -> dict:
    if not PREFERENCES_FILE.exists():
        return {}
    return json.loads(PREFERENCES_FILE.read_text())

# Helper: timezone-aware local now
def now_local() -> datetime:
    """
    Return current datetime localized to user's preferred timezone.
    Falls back to UTC safely.
    """
    prefs = load_preferences()
    tz_name = prefs.get("timezone", "UTC")
    try:
        return datetime.now(ZoneInfo(tz_name))
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc)

# Helper: enforce working hours from preferences
def within_work_hours(now: datetime) -> bool:
    """
    Returns True if 'now' is within work hours specified in preferences['work_hours'].
    """
    prefs = load_preferences()
    work_hours = prefs.get("work_hours", {})
    start = work_hours.get("start", "09:00")
    end = work_hours.get("end", "17:00")
    try:
        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))
        start_time = time(start_h, start_m)
        end_time = time(end_h, end_m)
        now_time = now.time()
        if start_time <= end_time:
            return start_time <= now_time < end_time
        else:
            # Overnight shift (e.g., 22:00-06:00)
            return now_time >= start_time or now_time < end_time
    except Exception:
        # If parsing fails, default to always within work hours.
        return True

# Helper: enforce per-channel hours from preferences
def channel_allowed_now(channel: str, now: datetime) -> bool:
    """
    Returns True if the given channel is allowed at the current time based on preferences.
    Falls back to general work hours if channel-specific hours are not defined.
    """
    prefs = load_preferences()
    channel_hours = prefs.get("channel_hours", {})
    hours = channel_hours.get(channel)

    if not hours:
        # Fallback to general work hours
        return within_work_hours(now)

    try:
        start = hours.get("start")
        end = hours.get("end")
        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))
        start_time = time(start_h, start_m)
        end_time = time(end_h, end_m)
        now_time = now.time()

        if start_time <= end_time:
            return start_time <= now_time < end_time
        else:
            # Overnight window
            return now_time >= start_time or now_time < end_time
    except Exception:
        return True

def load_tasks() -> Dict[str, dict]:
    if not TASKS_FILE.exists():
        return {}
    return json.loads(TASKS_FILE.read_text())

def save_tasks(tasks: Dict[str, dict]):
    TASKS_FILE.write_text(json.dumps(tasks, indent=2))

def load_decisions() -> Dict[str, list]:
    if not DECISIONS_FILE.exists():
        return {}
    return json.loads(DECISIONS_FILE.read_text())

def save_decisions(decisions: Dict[str, list]):
    DECISIONS_FILE.write_text(json.dumps(decisions, indent=2))

def record_successful_attempt(*, lead_id: str):
    """
    Record a successful interaction.
    Raises confidence, stabilizes channel, and records decision memory.
    """
    tasks = load_tasks()
    decisions = load_decisions()

    t = tasks.get(lead_id)
    if not t:
        return

    # Update task state
    t["attempts"] = max(0, t.get("attempts", 0) - 1)
    t["confidence"] = min(1.0, t.get("confidence", 0.5) + 0.15)
    t["channel"] = "call"

    # Record decision memory so downstream logic can reason
    decisions.setdefault(lead_id, []).append({
        "timestamp": datetime.utcnow().isoformat(),
        "outcome": "successful_contact",
        "notes": "Positive interaction",
    })

    save_decisions(decisions)
    save_tasks(tasks)

    # NEW: auto-generate next suggested action after success
    try:
        recommend_next_action(lead_id)
    except Exception:
        pass

# Task completion helper
def complete_task(lead_id: str, notes: Optional[str] = None) -> bool:
    """
    Mark a task as completed and record closure in decision memory.
    Completed tasks will no longer surface in daily focus or action queues.
    """
    tasks = load_tasks()
    decisions = load_decisions()

    t = tasks.get(lead_id)
    if not t:
        return False

    t["status"] = "completed"

    decisions.setdefault(lead_id, []).append({
        "timestamp": datetime.utcnow().isoformat(),
        "outcome": "task_completed",
        "notes": notes,
    })

    save_tasks(tasks)
    save_decisions(decisions)
    return True


def confidence_band(confidence: float) -> str:
    if confidence < 0.4:
        return "low"
    if confidence < 0.7:
        return "medium"
    return "high"


# New helper: format_confidence
def format_confidence(confidence: float) -> str:
    """
    Format confidence for display based on user preferences.
    """
    prefs = load_preferences()
    style = prefs.get("confidence_display", "percentage")

    if style == "band":
        return confidence_band(confidence)

    # default: percentage
    return f"{int(confidence * 100)}%"

def confidence_trend(history: list) -> str:
    """
    Return a simple confidence trend indicator based on recent outcomes.
    """
    if len(history) < 2:
        return "→"

    recent = [h.get("outcome") for h in history[-3:]]

    if "meeting_booked" in recent:
        return "↑"

    if recent.count("no_response") >= 2:
        return "↓"

    return "→"

def confidence_repair_hint(task: dict) -> Optional[str]:
    """
    Suggest a concrete action to raise confidence on a low/medium confidence task.
    """
    confidence = task.get("confidence", 0.5)
    attempts = task.get("attempts", 0)
    due_at = task.get("due_at")
    channel = task.get("channel", "call")

    if task.get("action_type") == "clarify":
        return "Ask a clarifying question to confirm the exact meeting day and time."

    # Preference-weighted bias: prefer call over email on low confidence + repeated attempts
    prefs = load_preferences()
    prefer_call_on_low_conf = prefs.get("prefer_call_on_low_confidence", True)

    if prefer_call_on_low_conf and confidence < 0.5 and attempts >= 2:
        return "Confidence is low after multiple attempts — switch to a call to increase signal."

    if confidence >= 0.7:
        return None

    if attempts >= 2:
        # Suggest WhatsApp for international leads where it's the norm
        country = task.get("country", "")
        tier = get_whatsapp_tier(country)
        if tier == 1 and channel != "whatsapp":
            return f"Consider switching to WhatsApp — it's the primary business channel in {country}."
        if tier == 2 and channel not in ("whatsapp", "call") and attempts >= 2:
            return f"Consider WhatsApp as a follow-up channel for {country}, or reframe the message."
        return f"Consider switching outreach channel (current: {channel}) or reframing the message."

    if not due_at:
        return "Clarify the next concrete step or timeline with the lead."

    return "Increase signal by confirming the next step or booking time."


# Decision-capture: update task based on meeting outcome
def apply_meeting_outcome(
    lead_id: str,
    outcome: str,
    notes: Optional[str] = None,
    meeting_date_iso: Optional[str] = None,
) -> None:
    """
    Update a task for the given lead based on the outcome of a meeting or outreach.
    Only one active task per lead is updated according to outcome rules.
    """
    tasks = load_tasks()
    decisions = load_decisions()
    history = decisions.setdefault(lead_id, [])
    t = tasks.get(lead_id)

    # Live-channel override: if clarification occurs on a call or in-person,
    # treat it as a confirmed meeting since clarification would happen live.
    live_channels = {"call", "in_person"}
    channel = (t or {}).get("channel", "call")
    if outcome == "needs_clarification" and channel in live_channels:
        outcome = "meeting_booked"

    # Only update one active task per lead
    if outcome == "meeting_booked":
        # Always create or update a meeting prep task when a meeting is booked
        try:
            meeting_dt = datetime.fromisoformat(meeting_date_iso) if meeting_date_iso else None
        except Exception:
            meeting_dt = None

        if meeting_dt:
            due_dt = meeting_dt - timedelta(hours=48)
        else:
            due_dt = None

        tasks[lead_id] = {
            "title": "Prepare agenda and discovery notes",
            "status": "open",
            "action_type": "prep",
            "focus_type": "event",
            "channel": "call",
            "attempts": 0,
            "confidence": min(1.0, max(0.6, (t or {}).get("confidence", 0.5) + 0.2)),
            "due_at": due_dt.isoformat() if due_dt else None,
            "lead_id": lead_id,
            "created_at": datetime.utcnow().isoformat(),
        }
    elif outcome == "needs_clarification":
        # Clarification loop:
        # If we're NOT on a live channel, create a clarification task.
        # If we ARE on a live channel, we should never reach this branch
        # because it would have been upgraded to meeting_booked above.
        now = now_local()
        prefs = load_preferences()
        work_hours = prefs.get("work_hours", {})
        start_str = work_hours.get("start", "09:00")
        start_h, start_m = map(int, start_str.split(":"))

        if within_work_hours(now):
            due_dt = now + timedelta(hours=1)
        else:
            next_day = now + timedelta(days=1)
            due_dt = next_day.replace(
                hour=start_h,
                minute=start_m,
                second=0,
                microsecond=0,
            )

        tasks[lead_id] = {
            "title": "Clarify meeting date and time",
            "status": "open",
            "action_type": "clarify",
            "focus_type": "project",
            "channel": (t or {}).get("channel", "email"),
            "attempts": 0,
            "confidence": max(0.0, (t or {}).get("confidence", 0.5) - 0.05),
            "due_at": due_dt.isoformat(),
            "lead_id": lead_id,
            "created_at": datetime.utcnow().isoformat(),
            "suggested_message": "Just confirming — which day works for you at that time?",
        }

        history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "outcome": "needs_clarification",
            "notes": "Confirmation present but missing explicit date or time.",
            "meeting_date_iso": meeting_date_iso,
        })

        save_decisions(decisions)
        save_tasks(tasks)
        return
    elif outcome == "interested_needs_approval":
        t["status"] = "waiting"
        # channel stays the same
        t["confidence"] = max(0.0, t.get("confidence", 0.5) - 0.05)
    elif outcome == "no_response":
        t["status"] = "open"
        t["attempts"] = t.get("attempts", 0) + 1
        t["confidence"] = max(0.0, t.get("confidence", 0.5) - 0.1)
    elif outcome == "lost":
        t["status"] = "lost"
        t["confidence"] = 0.0

    if notes:
        t["last_notes"] = notes

    history.append({
        "timestamp": datetime.utcnow().isoformat(),
        "outcome": outcome,
        "notes": notes,
        "meeting_date_iso": meeting_date_iso,
    })
    save_decisions(decisions)
    save_tasks(tasks)


def extract_meeting_signal(notes: str) -> dict:
    """
    Strict meeting signal extractor.

    A meeting is ONLY considered booked if:
      - A date reference is present
      - A time reference is present
      - AND a confirmation phrase is present

    Otherwise, we fall back to interest / no-response / lost.
    """
    notes_lc = notes.lower()

    confirmation_phrases = [
        # Strong confirmations
        "sounds good",
        "that works",
        "confirmed",
        "let’s do",
        "let's do",
        "we’re good",
        "we are good",
        "we're good",
        "see you",
        "see you then",
        "see you at",
        "locked in",
        "lock that in",
        "lock it in",

        # Explicit scheduling confirmations
        "we're locked in",
        "we are locked in",
        "all set",
        "we’re all set",
        "we are all set",
        "that’s confirmed",
        "that is confirmed",
        "we’ll do",
        "we will do",
        "works for us",
        "works for me",

        # Casual but high-signal confirmations
        "yep",
        "yep that works",
        "yes that works",
        "perfect",
        "perfect, see you",
        "great, see you",
        "ok see you",
        "okay see you",
        "cool, see you",
        "cool works",
        "cool that works",

        # Lock-in language
        "let’s lock that in",
        "let's lock that in",
        "let’s lock it in",
        "let's lock it in",
        "let’s lock this in",
        "let's lock this in",

        # Explicit agreement phrases
        "see you at",
        "see you there",
        "talk then",
        "catch up then",
        "that time works",
        "that works for us",
        "works on our end",
        "works on my end",
        "locked",
        "confirmed for",
    ]

    interest_phrases = [
        "need approval",
        "check internally",
        "run it by",
    ]

    lost_phrases = [
        "not interested",
        "no longer",
        "going another direction",
    ]

    # Naive date & time detection (strict on purpose)
    has_date = any(day in notes_lc for day in [
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
        "january", "february", "march", "april", "may",
        "june", "july", "august", "september", "october",
        "november", "december"
    ])

    has_time = any(token in notes_lc for token in [
        "am", "pm", "a.m", "p.m", ":"
    ])

    has_confirmation = any(p in notes_lc for p in confirmation_phrases)

    outcome = "no_response"

    # Attempt to resolve explicit weekday + time into a concrete datetime (timezone-aware)
    meeting_date_iso = None
    if has_date and has_time:
        try:
            prefs = load_preferences()
            tz = ZoneInfo(prefs.get("timezone", "UTC"))
            now = datetime.now(tz)

            weekdays = {
                "monday": 0,
                "tuesday": 1,
                "wednesday": 2,
                "thursday": 3,
                "friday": 4,
                "saturday": 5,
                "sunday": 6,
            }

            target_weekday = None
            for name, idx in weekdays.items():
                if name in notes_lc:
                    target_weekday = idx
                    break

            hour = None
            minute = 0
            if "am" in notes_lc or "pm" in notes_lc:
                import re
                m = re.search(r"(\d{1,2})(?::(\d{2}))?\s*(am|pm)", notes_lc)
                if m:
                    hour = int(m.group(1)) % 12
                    minute = int(m.group(2) or 0)
                    if m.group(3) == "pm":
                        hour += 12

            if target_weekday is not None and hour is not None:
                days_ahead = (target_weekday - now.weekday()) % 7
                if days_ahead == 0:
                    days_ahead = 7
                meeting_dt = (now + timedelta(days=days_ahead)).replace(
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                )
                meeting_date_iso = meeting_dt.isoformat()
        except Exception:
            meeting_date_iso = None

    # Fully confirmed meeting
    if has_date and has_time and has_confirmation:
        outcome = "meeting_booked"

    # Explicit clarification branch: confirmation and either time or date, but not both
    elif has_confirmation and (has_time or has_date):
        outcome = "needs_clarification"

    else:
        for phrase in interest_phrases:
            if phrase in notes_lc:
                outcome = "interested_needs_approval"
                break
        else:
            for phrase in lost_phrases:
                if phrase in notes_lc:
                    outcome = "lost"
                    break

    return {
        "outcome": outcome,
        "meeting_date_iso": meeting_date_iso
    }


# Helper: process meeting notes and wire into meeting outcome (DRY RUN only)
def process_meeting_notes(*, lead_id: str, notes: str) -> dict:
    """
    Process raw meeting notes for a lead:
    - Extract meeting signal
    - Apply outcome to task + decision memory
    - Return a compact summary for review
    """
    signal = extract_meeting_signal(notes)
    apply_meeting_outcome(
        lead_id=lead_id,
        outcome=signal.get("outcome"),
        meeting_date_iso=signal.get("meeting_date_iso"),
    )
    return {
        "lead_id": lead_id,
        "outcome": signal.get("outcome"),
        "meeting_date_iso": signal.get("meeting_date_iso"),
    }

def get_daily_focus(*, today_iso: str) -> Optional[str]:
    """
    Return one clear sentence describing what Hugh should focus on today.
    Rule: pick the single open task with the nearest due date.
    """
    tasks = load_tasks()
    open_tasks = []

    now = datetime.fromisoformat(today_iso)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    soon = now + timedelta(hours=48)

    for data in tasks.values():
        if data.get("status") not in ("open",):
            continue

        focus_type = data.get("focus_type", "event")
        due_at = data.get("due_at")

        # Project work surfaces daily, even without a due date
        if focus_type == "project":
            open_tasks.append(data)
            continue

        # Event-based work requires a due date
        if not due_at:
            continue

        # Event-based work only surfaces close to due date
        due_dt = datetime.fromisoformat(due_at)
        if due_dt.tzinfo is None:
            due_dt = due_dt.replace(tzinfo=timezone.utc)
        if due_dt <= soon:
            open_tasks.append(data)

    if not open_tasks:
        return "No critical tasks today. Stay available and move pipeline forward."

    # Sort by due date (earliest first)
    open_tasks.sort(key=lambda t: t["due_at"])

    t = open_tasks[0]
    band = confidence_band(t.get("confidence", 0.5))
    history = load_decisions().get(t["lead_id"], [])
    trend = confidence_trend(history)
    prefs = load_preferences()
    explain_low_confidence = prefs.get("explain_low_confidence", True)
    reason = ""
    if band == "low":
        if t.get("attempts", 0) >= 2:
            reason = "prior outreach attempts did not convert"
        else:
            reason = "next step not yet confirmed"
    elif band == "medium":
        if t.get("attempts", 0) == 0:
            reason = "new task with limited signal"
        else:
            reason = "partial engagement but outcome uncertain"
    base = (
        f"Today’s focus ({band}-confidence {trend}): {t['title']} "
        f"for lead {t['lead_id']} via {t.get('channel', 'call')} "
        f"(confidence: {format_confidence(t.get('confidence', 0.5))})."
    )
    repair = confidence_repair_hint(t)
    if repair:
        base += f" Suggested move: {repair}"
    if explain_low_confidence and band in ("low", "medium") and reason:
        base += f" Reason: {reason}."
    return base

def get_decision_memory(lead_id: str) -> list:
    decisions = load_decisions()
    return decisions.get(lead_id, [])


def summarize_decision_memory(lead_id: str) -> Optional[str]:
    """
    Return a short human-readable summary of decision history for a lead.
    Used for standups, confidence explanations, and coaching context.
    """
    history = get_decision_memory(lead_id)
    if not history:
        return None

    last = history[-1]
    outcome = last.get("outcome")
    notes = last.get("notes")

    if notes:
        return f"Last outcome: {outcome}. Notes: {notes}"
    return f"Last outcome: {outcome}."


# Gradual confidence adjustment based on decision history
def adjust_confidence_from_history(lead_id: str) -> None:
    """
    Gradually adjust confidence based on recent decision history.
    This prevents stale optimism and rewards recovered momentum.
    """
    tasks = load_tasks()
    decisions = load_decisions()

    t = tasks.get(lead_id)
    history = decisions.get(lead_id, [])

    if not t or len(history) < 2:
        return

    last_outcomes = [h.get("outcome") for h in history[-3:]]
    confidence = t.get("confidence", 0.5)

    # Repeated silence → decay confidence slightly
    if last_outcomes.count("no_response") >= 2:
        confidence = max(0.0, confidence - 0.05)

    # Recovery signal → restore confidence
    if "meeting_booked" in last_outcomes:
        confidence = min(1.0, confidence + 0.1)

    t["confidence"] = round(confidence, 2)
    save_tasks(tasks)


def can_auto_approve(task: dict, history: list) -> dict:
    """
    Decide whether a suggested task is safe to auto-approve.
    Returns: {"allowed": bool, "reasons": [str]}
    """
    reasons = []
    prefs = load_preferences()

    # Feature flag
    if not prefs.get("auto_approve_enabled", False):
        reasons.append("Auto-approve disabled in preferences.")

    # Confidence gate
    threshold = prefs.get("confidence_threshold_to_auto_act", 0.7)
    confidence = task.get("confidence", 0.5)
    if confidence < threshold:
        reasons.append(f"Confidence {format_confidence(confidence)} below threshold.")

    # Task type safety
    safe_types = {"follow_up", "prep", "recap"}
    if task.get("action_type") not in safe_types:
        reasons.append(f"Action type '{task.get('action_type')}' not eligible for auto-approve.")

    # Recent negative outcomes
    recent = [h.get("outcome") for h in history[-3:]]
    if any(o in ("no_response", "lost") for o in recent):
        reasons.append("Recent negative outcome present.")

    # Cooldown enforcement
    cooldown_hours = prefs.get("cooldown_hours_between_actions", 24)
    if history:
        last_ts = history[-1].get("timestamp")
        if last_ts:
            try:
                last_dt = datetime.fromisoformat(last_ts)
                now = now_local()
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                if now - last_dt < timedelta(hours=cooldown_hours):
                    reasons.append("Cooldown window not satisfied.")
            except Exception:
                pass

    # Channel + hours
    channel = task.get("channel", prefs.get("default_channel", "call"))
    if not channel_allowed_now(channel, now_local()):
        reasons.append(f"Channel '{channel}' not allowed at this time.")

    allowed = len(reasons) == 0
    return {"allowed": allowed, "reasons": reasons}

# Decision-aware next action recommender
def recommend_next_action(lead_id: str) -> Optional[dict]:
    """
    Decision-aware next action recommender.
    Creates a *suggested* task based on decision history, confidence, and attempts.
    """
    tasks = load_tasks()
    decisions = load_decisions()
    t = tasks.get(lead_id)
    if not t:
        return None

    history = decisions.get(lead_id, [])

    # Cooldown rule: avoid thrashing after success or completion
    if history:
        last_event = history[-1]
        last_outcome = last_event.get("outcome")
        last_ts = last_event.get("timestamp")

        if last_ts:
            try:
                last_dt = datetime.fromisoformat(last_ts)
                # COOLDOWN_HOURS now comes from preferences
                prefs = load_preferences()
                threshold = prefs.get("confidence_threshold_to_auto_act", 0.7)
                prefer_call_on_low = prefs.get("prefer_call_on_low_confidence", True)

                default_channel = prefs.get("default_channel", t.get("channel", "call"))
                if default_channel == "any":
                    default_channel = t.get("channel", "call")

                cooldown_hours = prefs.get("cooldown_hours_between_actions", 24)

                if last_outcome in ("successful_contact", "task_completed"):
                    if now_local() - last_dt < timedelta(hours=cooldown_hours):
                        return None
            except Exception:
                pass
    else:
        last_outcome = None

    # Enforce work hours after cooldown check, before generating suggestion
    if not within_work_hours(now_local()):
        return None

    confidence = t.get("confidence", 0.5)
    attempts = t.get("attempts", 0)

    prefs = load_preferences()
    threshold = prefs.get("confidence_threshold_to_auto_act", 0.7)
    prefer_call_on_low = prefs.get("prefer_call_on_low_confidence", True)

    default_channel = prefs.get("default_channel", t.get("channel", "call"))
    if default_channel == "any":
        default_channel = t.get("channel", "call")

    cooldown_hours = prefs.get("cooldown_hours_between_actions", 24)

    # Channel performance memory
    channel_stats = t.get("channel_success", {})
    best_channel = None
    if channel_stats:
        best_channel = max(channel_stats, key=channel_stats.get)

    if prefs.get("allow_channel_switching", True):
        if confidence < threshold and prefer_call_on_low:
            channel = "call"
        else:
            channel = best_channel or t.get("channel", default_channel)
    else:
        channel = default_channel

    # WhatsApp upgrade: if channel is text/SMS, check if WhatsApp is better
    # based on lead country. Respects manual overrides and Tier logic.
    if channel in ("text", "sms"):
        channel = recommend_messaging_channel(t)

    # Enforce per-channel hours
    if not channel_allowed_now(channel, now_local()):
        return None

    suggestion = None
    reason = None

    if t.get("status") == "lost":
        return {
            "action": "none",
            "reason": "Lead marked as lost. No further action recommended."
        }

    if last_outcome == "meeting_booked":
        suggestion = "Prepare agenda and discovery notes"
        reason = "Meeting is booked."

    elif last_outcome == "successful_contact":
        suggestion = "Send short recap and confirm next step"
        reason = "Positive contact made; reinforce momentum with a recap."

    elif last_outcome == "interested_needs_approval":
        suggestion = "Light follow-up in 3 days"
        reason = "Lead needs internal approval."

    elif last_outcome == "no_response":
        if attempts >= 2:
            suggestion = "Switch outreach channel"
            reason = f"No response after {attempts} attempts."
        else:
            suggestion = "Follow up gently"
            reason = "No response yet."

    elif last_outcome == "lost":
        return {
            "action": "close",
            "reason": "Lead is no longer interested."
        }

    if not suggestion:
        return None

    suggested_task = {
        "title": suggestion,
        "capability": t.get("capability"),
        "action_type": "suggested",
        "due_at": None,
        "lead_id": lead_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "suggested",
        "attempts": 0,
        "channel": channel,
        "focus_type": "project",
        "confidence": confidence,
        "reason": reason,
    }

    # Store suggestion separately without overwriting active task
    tasks[f"{lead_id}__suggested"] = suggested_task
    save_tasks(tasks)

    return {
        "action": suggestion,
        "reason": reason,
        "confidence": confidence,
    }


# Human-in-the-loop approval layer and message drafting helper
def draft_message(lead_id: str) -> Optional[str]:
    """
    Draft a suggested message based on the current task, confidence, and reason.
    This does NOT send anything. It only drafts text for human review.
    """
    tasks = load_tasks()
    t = tasks.get(lead_id)
    if not t:
        return None

    title = t.get("title", "")
    channel = t.get("channel", "email")
    confidence = format_confidence(t.get("confidence", 0.5))
    reason = t.get("reason") or t.get("last_notes") or ""

    if t.get("action_type") == "clarify":
        return (
            "Just confirming — which day works for you at that time?"
        )

    if channel == "email":
        return (
            f"Subject: Quick follow-up\n\n"
            f"Hi there — just wanted to follow up on our last touch.\n\n"
            f"Proposed next step: {title}.\n\n"
            f"Let me know what works best.\n\n"
            f"(confidence: {confidence})"
        )

    return (
        f"Call prep note: {title}. "
        f"Confidence {confidence}. {reason}"
    )



# Explainability helper for suggested tasks
def explain_suggested_task(lead_id: str) -> Optional[str]:
    """
    Return a human-readable explanation of why a suggested task exists
    and whether it is eligible for auto-approval.
    """
    tasks = load_tasks()
    decisions = load_decisions()
    key = f"{lead_id}__suggested"
    t = tasks.get(key)
    if not t:
        return None

    history = decisions.get(lead_id, [])
    verdict = can_auto_approve(t, history)

    lines = [
        f"Suggested action: {t.get('title')}",
        f"Channel: {t.get('channel')}",
        f"Confidence: {format_confidence(t.get('confidence', 0.5))}",
    ]

    if t.get("reason"):
        lines.append(f"Reason: {t['reason']}")

    status = "ALLOWED" if verdict["allowed"] else "BLOCKED"
    lines.append(f"Auto-approve: {status}")

    if verdict["reasons"]:
        for r in verdict["reasons"]:
            lines.append(f"- {r}")

    return "\n".join(lines)

# New helper: explain_active_task
def explain_active_task(lead_id: str) -> Optional[str]:
    """
    Explain why an active (open) task exists and why it is actionable now.
    """
    tasks = load_tasks()
    decisions = load_decisions()

    t = tasks.get(lead_id)
    if not t or t.get("status") != "open":
        return None

    history = decisions.get(lead_id, [])
    band = confidence_band(t.get("confidence", 0.5))
    trend = confidence_trend(history)
    now = datetime.utcnow()

    lines = [
        f"Active task: {t.get('title')}",
        f"Lead: {lead_id}",
        f"Status: open",
        f"Confidence: {format_confidence(t.get('confidence', 0.5))} ({band} {trend})",
        f"Channel: {t.get('channel')}",
    ]

    if t.get("due_at"):
        try:
            due_dt = datetime.fromisoformat(t["due_at"])
            lines.append(f"Due at: {due_dt.isoformat()}")
            if due_dt < now:
                lines.append("⚠️ Task is overdue.")
        except Exception:
            pass

    if within_work_hours(now):
        lines.append("Timing: Within allowed work hours.")
    else:
        lines.append("Timing: Outside work hours (task surfaced for awareness).")

    verdict = can_auto_approve(t, history)
    status = "ALLOWED" if verdict["allowed"] else "BLOCKED"
    lines.append(f"Auto-approve status: {status}")

    if verdict["reasons"]:
        for r in verdict["reasons"]:
            lines.append(f"- {r}")

    if history:
        last = history[-1]
        lines.append(
            f"Last outcome: {last.get('outcome')} — {last.get('notes') or 'no notes'}"
        )

    return "\n".join(lines)

def approve_suggested_task(lead_id: str) -> bool:
    """
    Approve and materialize a suggested task for a lead.
    Enforces work hours, cooldowns, and logs explicit human approval.
    """
    tasks = load_tasks()
    decisions = load_decisions()
    key = f"{lead_id}__suggested"
    t = tasks.get(key)
    if not t:
        return False

    history = decisions.get(lead_id, [])
    verdict = can_auto_approve(t, history)

    # Manual approval bypasses auto-approve feature flag but still respects hard safety rails
    hard_blocks = [
        r for r in verdict["reasons"]
        if not r.startswith("Auto-approve disabled")
    ]

    if hard_blocks:
        decisions.setdefault(lead_id, []).append({
            "timestamp": datetime.utcnow().isoformat(),
            "outcome": "approval_blocked",
            "notes": "; ".join(hard_blocks),
        })
        save_decisions(decisions)
        return False

    now = datetime.utcnow()
    due_dt = now + timedelta(days=3)

    tasks[lead_id] = {
        **t,
        "status": "open",
        "due_at": due_dt.isoformat(),
        "action_type": "follow_up",
        "created_at": now.isoformat(),
    }

    decisions.setdefault(lead_id, []).append({
        "timestamp": now.isoformat(),
        "outcome": "approved_by_human",
        "notes": f"Approved suggestion: {t.get('title')}",
    })

    # Adapter stub: CRM task creation
    crm_create_task({
        "lead_id": lead_id,
        "title": tasks[lead_id]["title"],
        "due_at": tasks[lead_id]["due_at"],
        "channel": tasks[lead_id].get("channel"),
        "confidence": tasks[lead_id].get("confidence"),
    })

    del tasks[key]
    save_tasks(tasks)
    save_decisions(decisions)
    return True


def reject_suggested_task(lead_id: str, reason: Optional[str] = None) -> bool:
    """
    Reject a suggested task and record the decision.
    """
    tasks = load_tasks()
    decisions = load_decisions()

    key = f"{lead_id}__suggested"
    if key not in tasks:
        return False

    decisions.setdefault(lead_id, []).append({
        "timestamp": datetime.utcnow().isoformat(),
        "outcome": "suggestion_rejected",
        "notes": reason,
    })

    del tasks[key]
    save_decisions(decisions)
    save_tasks(tasks)
    return True
def materialize_suggested_tasks(*, now_iso: str) -> int:
    """
    Convert suggested tasks (status='suggested') into active open tasks with due dates.
    Returns the number of tasks materialized.
    """
    tasks = load_tasks()
    now = datetime.fromisoformat(now_iso)
    created = 0

    for key, t in list(tasks.items()):
        if not key.endswith("__suggested"):
            continue
        if t.get("status") != "suggested":
            continue

        # Default scheduling rules
        # Light follow-ups: +3 days
        due_dt = now + timedelta(days=3)

        new_key = t["lead_id"]
        tasks[new_key] = {
            **t,
            "status": "open",
            "due_at": due_dt.isoformat(),
            "action_type": "follow_up",
            "created_at": datetime.utcnow().isoformat(),
        }

        # Remove the suggestion entry
        del tasks[key]
        created += 1

    save_tasks(tasks)
    return created


# Action queue helper: list actionable tasks in priority order

# Inserted helper function for daily_command

# ---- Urgency-based prioritization ----
def compute_urgency(task: dict, now: datetime) -> int:
    """
    Compute urgency score (0-100) for a task based on type, due date, confidence, attempts, cooldown, etc.
    """
    urgency = 0
    capability = (task.get("capability") or "").lower()
    action_type = (task.get("action_type") or "").lower()
    focus_type = (task.get("focus_type") or "").lower()
    project_value = (task.get("project_value") or "maintenance").lower()
    channel = task.get("channel", "call")
    due_at = task.get("due_at")
    confidence = float(task.get("confidence", 0.5))
    attempts = int(task.get("attempts", 0))
    # Sales/prospecting action types
    sales_types = {"prep", "follow_up", "call", "recap"}
    if any(x in capability for x in sales_types) or action_type in sales_types:
        urgency += 40
    if action_type == "prep":
        urgency += 30
    if action_type in ("follow_up", "recap"):
        urgency += 20
    if focus_type == "project":
        if project_value == "compounding":
            urgency += 5  # only lightly urgent, surfaces when pipeline is quiet
        else:
            urgency -= 10  # maintenance projects are de-prioritized
    if due_at:
        try:
            due_dt = datetime.fromisoformat(due_at)
            if due_dt.tzinfo is None:
                due_dt = due_dt.replace(tzinfo=now.tzinfo or timezone.utc)
            if due_dt < now:
                urgency += 10  # overdue
            else:
                delta = due_dt - now
                if delta.total_seconds() <= 24 * 3600:
                    urgency += 25
                elif delta.total_seconds() <= 48 * 3600:
                    urgency += 15
        except Exception:
            pass
    if confidence >= 0.7:
        urgency += 10
    if attempts >= 1:
        urgency += 5
    # Cooldown logic (reuse logic from can_auto_approve: check last attempt time)
    decisions = load_decisions()
    history = decisions.get(task.get("lead_id"), [])
    prefs = load_preferences()
    cooldown_hours = prefs.get("cooldown_hours_between_actions", 24)
    cooldown_active = False
    if history:
        last_ts = history[-1].get("timestamp")
        if last_ts:
            try:
                last_dt = datetime.fromisoformat(last_ts)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=now.tzinfo or timezone.utc)
                if now - last_dt < timedelta(hours=cooldown_hours):
                    cooldown_active = True
            except Exception:
                pass
    if cooldown_active:
        urgency -= 30
    # Channel not allowed now
    if not channel_allowed_now(channel, now):
        urgency -= 20
    urgency = max(0, min(100, urgency))
    return urgency

def get_action_queue(*, now_iso: str) -> list:
    """
    Return a prioritized list of human-readable actions that are due or actionable, sorted by urgency.
    """
    tasks = load_tasks()
    now = datetime.fromisoformat(now_iso)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo(load_preferences().get("timezone", "UTC")))
    if not within_work_hours(now):
        return []
    soon = now + timedelta(hours=48)
    queue = []
    has_sales_work = any(
        t.get("status") == "open"
        and t.get("focus_type") != "project"
        for t in tasks.values()
    )
    for t in tasks.values():
        if t.get("status") != "open":
            continue
        # Only consider actionable tasks for urgency
        due_at = t.get("due_at")
        focus_type = t.get("focus_type", "event")
        channel = t.get("channel", "call")
        if focus_type == "project":
            if has_sales_work:
                continue  # projects only surface when pipeline is quiet
        elif due_at and datetime.fromisoformat(due_at) <= soon:
            pass
        else:
            continue
        urgency = compute_urgency(t, now)
        if urgency <= 0:
            continue
        confidence = t.get("confidence", 0.5)
        queue.append({
            "urgency": urgency,
            "confidence": confidence,
            "title": t.get("title", ""),
            "lead_id": t.get("lead_id", ""),
            "channel": channel,
            "action_type": t.get("action_type", ""),
            "focus_type": focus_type,
        })
    # Sort by urgency descending
    queue.sort(key=lambda x: x["urgency"], reverse=True)
    # Format string
    formatted = []
    for item in queue:
        formatted.append(
            f"[{item['urgency']}% urgent] {item['title']} → lead {item['lead_id']} via {item['channel']}"
        )
    return formatted


# Return only the top action (highest urgency)
def next_action(now_iso: Optional[str] = None) -> Optional[str]:
    """
    Returns the top next action as a human-readable string with urgency. Returns None if no actions.
    """
    if not now_iso:
        now_iso = now_local().isoformat()
    queue = get_action_queue(now_iso=now_iso)
    if not queue:
        return None
    return queue[0]


# Lightweight helper for ultra-quick check-ins
def what_next(now_iso: Optional[str] = None) -> Optional[str]:
    """
    Ultra-light check-in during the day.
    Returns ONE action to do right now, prioritizing sales/prospecting.
    If clarification is needed, surface the exact clarifying question.
    """
    tasks = load_tasks()

    if not now_iso:
        now = now_local()
    else:
        now = datetime.fromisoformat(now_iso)
        if now.tzinfo is None:
            now = now.replace(tzinfo=ZoneInfo(load_preferences().get("timezone", "UTC")))

    # 1. Surface clarification immediately if present
    for t in tasks.values():
        if t.get("status") == "open" and t.get("action_type") == "clarify":
            question = t.get("suggested_message") or "Clarify the next step."
            return f"Clarify now → {question}"

    # 2. Otherwise fall back to urgency-based next action
    action = next_action(now_iso=now.isoformat())
    if action:
        return action

    # 3. Nothing urgent → strategic guidance
    return "No urgent sales actions. Work on strategic projects or prospecting."

#
# =========================
# Adapter stubs (DRY RUN)
# =========================

def crm_create_task(payload: dict) -> None:
    mode = get_integration_mode()
    if mode == "dry_run":
        print("[DRY RUN][CRM] Would create task:", payload)
        return
    # live implementation placeholder
    print("[LIVE][CRM] Task created:", payload)


def crm_log_activity(payload: dict) -> None:
    mode = get_integration_mode()
    if mode == "dry_run":
        print("[DRY RUN][CRM] Would log activity:", payload)
        return
    print("[LIVE][CRM] Activity logged:", payload)


def comms_draft_message(payload: dict) -> None:
    mode = get_integration_mode()
    if mode == "dry_run":
        print("[DRY RUN][COMMS] Would draft message:", payload)
        return
    print("[LIVE][COMMS] Message drafted:", payload)


#
# Daily command for AI Hugh
def daily_command(now_iso: str) -> str:
    """
    Single daily entry point for AI Hugh.
    Shows top next action with urgency and confidence.
    """
    focus = get_daily_focus(today_iso=now_iso)
    auto_line = None
    try:
        tasks = load_tasks()
        for t in tasks.values():
            if t.get("status") == "open":
                history = load_decisions().get(t.get("lead_id"), [])
                verdict = can_auto_approve(t, history)
                status = "ALLOWED" if verdict["allowed"] else "BLOCKED"
                auto_line = f"Auto-approve check: {status}"
                if verdict["reasons"]:
                    auto_line += " | Reasons: " + "; ".join(verdict["reasons"])
                break
    except Exception:
        pass

    now_dt = datetime.fromisoformat(now_iso)
    if now_dt.tzinfo is None:
        now_dt = now_local()
    status = (
        "Inside allowed hours"
        if within_work_hours(now_dt)
        else "Outside allowed hours — actions may be deferred by channel rules."
    )
    lines = [
        "🤖 AI Hugh — Daily Brief",
        f"Status: {status}",
        "",
        focus,
        "",
    ]
    # --- EXPLANATION FOR SUGGESTED TASKS ---
    try:
        for t in load_tasks().values():
            if t.get("status") == "suggested":
                expl = explain_suggested_task(t.get("lead_id"))
                if expl:
                    lines.append("")
                    lines.append("Why this suggestion:")
                    lines.append(expl)
                break
    except Exception:
        pass
    # ---------------------------------------
    if auto_line:
        lines.append(auto_line)
        lines.append("")
    lines.append("Next action:")
    # Show only the top next action, with urgency and confidence
    top_task = None
    # Find the top actionable task for details
    try:
        tasks = load_tasks()
        now = now_dt
        max_urgency = -1
        for t in tasks.values():
            if t.get("status") != "open":
                continue
            urgency = compute_urgency(t, now)
            if urgency > max_urgency:
                max_urgency = urgency
                top_task = t
        if top_task and max_urgency > 0:
            confidence = top_task.get("confidence", 0.5)
            lines.append(
                f"- [{max_urgency}% urgent | {format_confidence(confidence)} confidence] {top_task.get('title', '')} → lead {top_task.get('lead_id', '')} via {top_task.get('channel', 'call')}"
            )
        else:
            lines.append("- No actions queued")
    except Exception:
        lines.append("- No actions queued")
    lines.append("")
    lines.append("Say 'approve' to materialize suggestions.")
    return "\n".join(lines)
#
# =========================
# Scenario Runner (TEST ONLY)
# =========================

def run_scenario(
    *,
    lead_id: str,
    notes: str,
    now_iso: str,
) -> None:
    """
    Deterministic test harness.
    Runs notes through the engine and prints the resulting state.
    """
    print("\n--- SCENARIO RUN ---")

    print("Notes:")
    print(notes)
    print("")

    result = process_meeting_notes(
        lead_id=lead_id,
        notes=notes,
    )

    tasks = load_tasks()

    task = tasks.get(lead_id)

    print("Outcome:", result.get("outcome"))
    print("Meeting date:", result.get("meeting_date_iso"))
    print("")

    if not task:
        print("No active task created.")
        return

    print("Active Task:")
    print(f"- Title: {task.get('title')}")
    print(f"- Status: {task.get('status')}")
    print(f"- Channel: {task.get('channel')}")
    print(f"- Due at: {task.get('due_at')}")
    print(f"- Confidence: {format_confidence(task.get('confidence', 0.5))}")

    explanation = explain_active_task(lead_id)
    if explanation:
        print("\nExplanation:")
        print(explanation)

    print("\nDaily Brief Preview:")
    print(daily_command(now_iso=now_iso))

    print("\n--- END SCENARIO ---\n")