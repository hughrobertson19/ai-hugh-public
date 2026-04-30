"""
note_processor.py
-----------------
Converts structured meeting notes into actionable decisions, commitments, and risks.

This is NOT a note-capture system. The note source already handles summaries.
This module extracts ONLY what drives action and prevents dropped responsibilities:
  - Commitments with clear owners
  - Risks and revenue signals
  - Follow-up gaps (unclear owners, missing deadlines)
  - Internal expectations on Hugh

No AI. No external APIs. No guessing.
If something is unclear, it goes in follow_up_gaps or missing_fields.

Usage:
    python3 core/note_processor.py path/to/note.txt
    python3 core/note_processor.py path/to/note.txt --compact
"""

import re
import json
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration — edit these to tune extraction without touching logic
# ---------------------------------------------------------------------------

# All name variants that mean Hugh. Add initials or nicknames if they appear.
# ── FORMAT HOOK ────────────────────────────────────────────────────────────
HUGH_NAMES = {"hugh", "hugh robertson", "hugh r"}

# Words in a meeting title or body that suggest an internal meeting.
# ── NOTE ── "manager" and "planning" excluded — too common in prospect job titles/contexts.
INTERNAL_SIGNALS = {
    "huddle", "standup", "stand-up", "sync", "team meeting", "team call",
    "internal", "1:1", "one on one", "all hands", "all-hands",
    "check-in", "check in", "retrospective", "retro",
}

# Words that suggest an external (prospect/customer) meeting.
EXTERNAL_SIGNALS = {
    "discovery", "demo", "demonstration", "prospect", "client", "customer",
    "follow-up", "follow up", "intro call", "introductory", "exploratory",
    "proposal", "rfp", "onboarding", "kickoff", "kick-off",
}

# Known internal colleagues by first name or full name.
# Populate with your team's first names to filter internal mentions.
INTERNAL_NAMES = set()

# Positive revenue signal phrases.
POSITIVE_SIGNALS = [
    "ready to move forward", "budget approved", "greenlit", "green light",
    "signed off", "approved to move", "when can we start", "let's proceed",
    "moving forward", "start date", "kick off", "we want to", "we'd like to",
    "fits our needs", "this solves", "see the value", "strong interest",
    "interested", "evaluating", "exploring",
]

# Risk / negative signal phrases.
RISK_SIGNALS = [
    "not until", "waiting on approval", "needs sign off", "budget concern",
    "competitor", "rfp", "evaluating others", "no budget", "tight budget",
    "data migration", "legacy system", "integration complexity",
    "no timeline", "unclear timeline", "after the merger",
]

# Deadline phrases that count as time signals (required for a task to be extracted).
DEADLINE_PATTERNS = [
    r"\bby\s+(?:EOD|end of day|end of business|EOB|COB)\b",
    r"\bby\s+(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b",
    r"\bby\s+(?:today|tomorrow)\b",
    r"\bend of (?:day|business|week|month)\b",
    r"\bthis\s+(?:week|month|quarter)\b",
    r"\bnext\s+(?:week|month|quarter)\b",
    r"\bbefore\s+\w[\w\s]{1,20}\b",
    # ── FORMAT HOOK ── "by April 3" or "by April 3, 2026"
    r"\bby\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?"
    r"|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+\d{1,2}(?:,?\s+\d{4})?\b",
    r"\bby\s+\w[\w\s]{1,15}\d{4}\b",
    r"\bASAP\b",
    r"\burgently\b",
    r"\bimmediately\b",
    r"\btoday\b",
    r"\btomorrow\b",
]

# Hugh's core work domains — used to detect when a colleague's task blocks him.
HUGH_WORK_DOMAINS = {
    "outreach", "calls", "email", "pipeline", "deal", "demo", "pitch",
    "proposal", "rfp", "meeting", "prospect", "client", "campaign",
    "account", "opportunity", "scoping", "engagement",
}

# Patterns that show a colleague's task enables Hugh's next step.
_ENABLING_PATTERNS = [
    r"\bso (?:we|i|hugh) can\b",
    r"\bfor (?:the )?(?:demo|call|meeting|scoping|integration|"
    r"engagement|deal|pitch|proposal|review|next step)\b",
    r"\bbefore (?:the )?(?:demo|call|meeting|next)\b",
    r"\bneeded for\b",
    r"\brequired for\b",
]

# Patterns that show a colleague's task blocks Hugh's work.
_BLOCKING_PATTERNS = [
    r"\bblock(?:s|ing)?\b",
    r"\baffect(?:s|ing)?\s+(?:" + "|".join(sorted(HUGH_WORK_DOMAINS)) + r")\b",
    r"\bbefore Hugh\b",
]

# C5 — execution impact (no Hugh mention required)
# Systems and data Hugh depends on for SDR execution.
_C5_EXECUTION_SYSTEMS = {
    "caller id", "call display", "outbound call", "phone system", "work phone",
    "crm", "salesforce", "email system", "dialer",
    "prospect list", "prospect data", "contact data", "lead data",
    "tool access", "license", "login", "access to",
}
# Verbs indicating a fix/restore/enable action (not routine updates).
_C5_FIX_VERBS = {
    "fix", "resolve", "repair", "restore", "troubleshoot",
    "enable", "set up", "configure", "provision", "unlock", "grant",
}

# Words that make a line too vague to extract as a task.
VAGUE_PHRASES = [
    "focus on", "think about", "consider", "keep in mind", "be aware",
    "look into", "explore the possibility", "might", "potentially",
    "when possible", "if time allows", "at some point",
]

# Section header keyword → canonical name.
# ── FORMAT HOOK ── Add headers as you observe new note formats.
SECTION_HEADERS = {
    "action items": "action_items",
    "action item": "action_items",
    "actions": "action_items",
    "follow-ups": "action_items",
    "follow ups": "action_items",
    "to-dos": "action_items",
    "todos": "action_items",
    "next steps": "next_steps",
    "next step": "next_steps",
    "attendees": "attendees",
    "participants": "attendees",
    "in attendance": "attendees",
    "present": "attendees",
    "blockers": "blockers",
    "concerns": "blockers",
    "risks": "blockers",
    "summary": "summary",
    "overview": "summary",
    "key points": "summary",
    "key takeaways": "summary",
    "discussion": "discussion",
    "notes": "discussion",
    "decisions": "decisions",
}


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = text.replace("\u201c", '"').replace("\u201d", '"')
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = text.replace("\u00a0", " ")
    return "\n".join(line.rstrip() for line in text.split("\n"))


def is_bullet(line: str) -> bool:
    return bool(re.match(r"^\s*[-*\u2022\u2013\u25e6\u25aa\u25b8]\s", line))


def strip_bullet(line: str) -> str:
    return re.sub(r"^\s*[-*\u2022\u2013\u25e6\u25aa\u25b8]\s+", "", line).strip()


def detect_sections(text: str) -> dict:
    """
    Split note into named sections by header keywords.
    Lines before the first header → 'header'.

    ── FORMAT HOOK ──
    Common formats use ## Header, **Header**, or plain Header: formats.
    """
    lines = text.split("\n")
    sections: dict = {"header": []}
    current = "header"

    for line in lines:
        stripped = line.strip()
        if not stripped:
            sections.setdefault(current, []).append(line)
            continue

        # Normalise: strip markdown, trailing colon
        cleaned = re.sub(r"^\#{1,6}\s*", "", stripped)
        cleaned = re.sub(r"\*{1,2}(.*?)\*{1,2}", r"\1", cleaned)
        cleaned = cleaned.rstrip(":").strip().lower()

        canonical = SECTION_HEADERS.get(cleaned)
        if canonical:
            current = canonical
            sections.setdefault(current, [])
        else:
            sections.setdefault(current, []).append(line)

    return sections


# ---------------------------------------------------------------------------
# Step 1 — Meeting type detection
# ---------------------------------------------------------------------------

def detect_meeting_type(sections: dict, raw_text: str) -> str:
    """
    Classify meeting as 'external' or 'internal'.

    Rules (in order):
    1. Title/header contains an internal signal word → internal
    2. Title/header contains an external signal word → external
    3. Only internal colleague names appear in attendees → internal
    4. Default → external (safer for SDR context)
    """
    header_lines = sections.get("header", [])
    title_text = " ".join(header_lines[:3]).lower()
    sample = (title_text + " " + raw_text[:300]).lower()

    if any(sig in sample for sig in INTERNAL_SIGNALS):
        return "internal"
    if any(sig in sample for sig in EXTERNAL_SIGNALS):
        return "external"

    # Attendee-based fallback: if all named attendees are known internal staff
    attendee_lines = [
        strip_bullet(l) for l in sections.get("attendees", []) if l.strip()
    ]
    if attendee_lines:
        names_lower = {l.split("(")[0].strip().lower() for l in attendee_lines if l}
        if names_lower and all(
            any(n in name for n in INTERNAL_NAMES | HUGH_NAMES)
            for name in names_lower
        ):
            return "internal"

    return "external"  # default — SDR context biases toward external


# ---------------------------------------------------------------------------
# Step 2 — Date extraction
# ---------------------------------------------------------------------------

def extract_date(text: str) -> Optional[str]:
    """Extract date from text, return YYYY-MM-DD or None."""
    # ISO
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m:
        return m.group(0)

    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    mp = "|".join(months.keys())

    # "March 26, 2026"
    m = re.search(rf"\b({mp})\s+(\d{{1,2}}),?\s+(\d{{4}})\b", text, re.IGNORECASE)
    if m:
        return f"{int(m.group(3)):04d}-{months[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"

    # "26 March 2026"
    m = re.search(rf"\b(\d{{1,2}})\s+({mp})\s+(\d{{4}})\b", text, re.IGNORECASE)
    if m:
        return f"{int(m.group(3)):04d}-{months[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"

    # ── FORMAT HOOK ── Short year: "Thu, 26 Mar 26"
    m = re.search(rf"\b(\d{{1,2}})\s+({mp})\s+(\d{{2}})\b", text, re.IGNORECASE)
    if m:
        yy = int(m.group(3))
        year = 2000 + yy if yy <= 50 else 1900 + yy
        return f"{year:04d}-{months[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"

    return None


# ---------------------------------------------------------------------------
# Step 3 — Commitment extraction
# ---------------------------------------------------------------------------

def _has_time_signal(line: str) -> bool:
    """Return True if the line contains a recognisable deadline or urgency phrase."""
    line_lower = line.lower()
    return any(re.search(p, line_lower, re.IGNORECASE) for p in DEADLINE_PATTERNS)


def _extract_deadline(line: str) -> Optional[str]:
    """Return the deadline phrase found in the line, or None."""
    for pattern in DEADLINE_PATTERNS:
        m = re.search(pattern, line, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


def _is_too_vague(action: str) -> bool:
    """Return True if the action is too vague to be a real task."""
    action_lower = action.lower()
    if len(action.split()) < 3:
        return True
    return any(phrase in action_lower for phrase in VAGUE_PHRASES)


# Words that look like capitalised names but are not people.
_OWNER_STOPWORDS = {
    "option", "note", "meeting", "action", "next", "step", "follow",
    "agenda", "summary", "update", "decision", "goal", "item", "task",
    "question", "issue", "topic", "context", "background", "overview",
}


def _classify_owner(line: str) -> tuple:
    """
    Determine commitment owner from line text.
    Returns ("hugh", None) | ("other", name) | ("unclear", None).

    ── FORMAT HOOK ──
    Notes "Next Steps" format: "- Name: task" or "- Name will task"
    """
    # "Name: action" format
    m = re.match(r"^([A-Za-z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*:\s*(.+)", line)
    if m:
        name = m.group(1).strip()
        if name.lower() in _OWNER_STOPWORDS:
            return "unclear", None
        if name.lower() in HUGH_NAMES:
            return "hugh", None
        return "other", name

    # "Name will/to/should action"
    m = re.match(
        r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s+(?:will|to|needs to|should)\s+.+",
        line
    )
    if m:
        name = m.group(1).strip()
        if name.lower() in _OWNER_STOPWORDS:
            return "unclear", None
        if name.lower() in HUGH_NAMES:
            return "hugh", None
        return "other", name

    # Unambiguous Hugh reference anywhere in line
    if re.search(r"\bhugh\b", line, re.IGNORECASE):
        return "hugh", None

    return "unclear", None


def _strip_owner_prefix(line: str) -> str:
    """Remove 'Name: ' or 'Name will ' prefix, return clean action text."""
    line = re.sub(r"^[A-Za-z][a-z]+(?:\s+[A-Z][a-z]+)?\s*:\s*", "", line)
    line = re.sub(
        r"^[A-Za-z][a-z]+(?:\s+[A-Z][a-z]+)?\s+(?:will|to|needs to|should)\s+",
        "", line, flags=re.IGNORECASE
    )
    line = re.sub(r"^(?:will|to|needs to|should)\s+", "", line, flags=re.IGNORECASE)
    return line.strip()


def _affects_hugh_execution(line: str, task: str, hugh_source_lines: list) -> bool:
    """
    Return True only if this person's task directly impacts Hugh's ability to execute.

    C1 — explicit Hugh reference in the source line or task
    C2 — enabling language (so we can, for the demo, needed for...)
    C3 — blocking language affecting Hugh's work domain
    C4 — owner name appears in one of Hugh's own extracted tasks (required for Hugh's step)
    C5 — execution impact: fixes or restores a system/data Hugh depends on,
         even without an explicit Hugh reference.
         Test: "If this person fails, does Hugh lose time, pipeline, or credibility?"

    If none apply, the person should be discarded.
    """
    combined = (line + " " + task).lower()

    # C1: explicit Hugh reference
    if re.search(r"\bhugh\b", combined):
        return True

    # C2: enabling language — task makes Hugh's next step possible
    if any(re.search(p, combined) for p in _ENABLING_PATTERNS):
        return True

    # C3: blocking language — task failure would stall Hugh's work
    if any(re.search(p, combined, re.IGNORECASE) for p in _BLOCKING_PATTERNS):
        return True

    # C4: owner's first name appears in one of Hugh's own task lines
    #     (i.e., Hugh's task requires this person — "Demo with Alex")
    first_token = re.split(r"[\s:]", line.strip())[0].lower()
    if len(first_token) >= 3:
        for source in hugh_source_lines:
            if first_token in source.lower():
                return True

    # C5: task fixes or restores a system/data Hugh depends on for execution.
    #     Requires both a fix-type verb AND a recognised execution system.
    #     Routine updates (log, record, update) do NOT trigger this condition.
    has_fix = any(verb in combined for verb in _C5_FIX_VERBS)
    has_system = any(system in combined for system in _C5_EXECUTION_SYSTEMS)
    if has_fix and has_system:
        return True

    return False


def extract_commitments(sections: dict) -> tuple:
    """
    Parse action items and next steps into:
      - hugh_owns: [{task, due, source}]
      - others_owe_hugh: [{owner, task, due}]
      - gaps: [{context, issue, recommended_action}]

    Only extracts tasks where:
      (1) owner is clear  (2) action is specific  (3) time signal OR clear urgency

    Unclear or vague items go to gaps.
    """
    source_lines = (
        sections.get("action_items", [])
        + sections.get("next_steps", [])
        + sections.get("decisions", [])
    )

    # First pass — collect Hugh's raw source lines so _affects_hugh_execution
    # can check whether a colleague's name appears in Hugh's own tasks (C4).
    hugh_source_lines: list = []
    for raw_line in source_lines:
        if not raw_line.strip():
            continue
        line = strip_bullet(raw_line) if is_bullet(raw_line) else raw_line.strip()
        if len(line) < 6:
            continue
        owner_type, _ = _classify_owner(line)
        if owner_type == "hugh":
            hugh_source_lines.append(line)

    hugh_owns = []
    others_owe = []
    gaps = []

    for raw_line in source_lines:
        if not raw_line.strip():
            continue
        line = strip_bullet(raw_line) if is_bullet(raw_line) else raw_line.strip()
        if len(line) < 6:
            continue
        # Skip markdown headers and inline comments — not action items
        if line.startswith("#"):
            continue

        owner_type, owner_name = _classify_owner(line)
        action = _strip_owner_prefix(line)
        deadline = _extract_deadline(line)
        has_time = _has_time_signal(line)
        too_vague = _is_too_vague(action)

        if too_vague:
            gaps.append({
                "context": line,
                "issue": "no next step",
                "recommended_action": "Clarify what the specific action is.",
            })
            continue

        if owner_type == "unclear":
            gaps.append({
                "context": line,
                "issue": "unclear owner",
                "recommended_action": "Confirm who owns this before it drops.",
            })
            continue

        if not has_time:
            # Still extract it but also flag it as lacking a deadline
            gaps.append({
                "context": line,
                "issue": "weak commitment",
                "recommended_action": "No deadline attached — confirm timing.",
            })

        if owner_type == "hugh":
            hugh_owns.append({
                "task": action,
                "due": deadline,
                "source": line,
            })
        else:
            # Only track colleagues who directly affect Hugh's execution.
            # Prefer missing someone over including irrelevant people.
            if not _affects_hugh_execution(line, action, hugh_source_lines):
                continue
            others_owe.append({
                "owner": owner_name,
                "task": action,
                "due": deadline,
                "owner_evidence": line,
            })

    return hugh_owns, others_owe, gaps


# ---------------------------------------------------------------------------
# Step 4 — Revenue signals (external meetings)
# ---------------------------------------------------------------------------

def extract_revenue_signals(sections: dict, raw_text: str, title: str) -> list:
    """
    Identify positive buying signals and risks from external meeting notes.
    Returns list of {account, signal, risk, recommended_action}.

    Only runs meaningfully for external meetings.
    ── FORMAT HOOK ── Add domain-specific phrases to POSITIVE_SIGNALS and
    RISK_SIGNALS at the top of this file.
    """
    signals = []
    text_lower = raw_text.lower()

    # Infer account from title ("Meeting with Acme" or "Acme - Discovery Call")
    account = None
    m = re.search(r"(?:with|@|-)\s*([A-Z][A-Za-z &]{2,30})", title)
    if m:
        candidate = m.group(1).strip()
        if len(candidate.split()) <= 4:
            account = candidate

    found_positive = [p for p in POSITIVE_SIGNALS if p in text_lower]
    found_risks = [r for r in RISK_SIGNALS if r in text_lower]

    if found_positive:
        signals.append({
            "account": account,
            "signal": f"Positive indicators: {', '.join(found_positive[:3])}",
            "risk": None,
            "recommended_action": "Follow up promptly — momentum is present.",
        })

    if found_risks:
        signals.append({
            "account": account,
            "signal": None,
            "risk": f"Risk indicators: {', '.join(found_risks[:3])}",
            "recommended_action": "Address blockers before advancing the deal.",
        })

    # Blockers section → explicit risk entries
    for line in sections.get("blockers", []):
        cleaned = strip_bullet(line) if is_bullet(line) else line.strip()
        if cleaned and len(cleaned) > 8:
            signals.append({
                "account": account,
                "signal": None,
                "risk": cleaned,
                "recommended_action": "Investigate and resolve before next meeting.",
            })

    return signals


# ---------------------------------------------------------------------------
# Step 5 — Internal alignment (internal meetings)
# ---------------------------------------------------------------------------

def extract_internal_alignment(sections: dict, raw_text: str) -> dict:
    """
    Extract what matters for internal meetings:
      - expectations_on_hugh: things Hugh is explicitly expected to do/report
      - team_dependencies: things Hugh is waiting on from others
      - reporting_requirements: stats, deadlines, submission requirements

    ── FORMAT HOOK ── Huddle notes often list these as:
    "Hugh: [expectation]" or "Team: submit X by [deadline]"
    """
    expectations = []
    dependencies = []
    reporting = []

    # Reporting requirement signals — kept specific to avoid false positives.
    reporting_keywords = [
        "submit", "send stats", "by end of day", "by eod",
        "end of business", "reporting requirements", "report by", "update by",
    ]

    # Skip attendees and header sections — they don't contain actionable expectations.
    skip_sections = {"attendees", "header"}
    all_lines = []
    for section_name, section_lines in sections.items():
        if section_name not in skip_sections:
            all_lines.extend(section_lines)

    for raw_line in all_lines:
        line = strip_bullet(raw_line) if is_bullet(raw_line) else raw_line.strip()
        if not line or len(line) < 6:
            continue
        # Skip markdown section headers (they are content, not requirements)
        if line.startswith("#"):
            continue

        line_lower = line.lower()

        # Expectations on Hugh
        owner_type, _ = _classify_owner(line)
        if owner_type == "hugh":
            action = _strip_owner_prefix(line)
            if not _is_too_vague(action):
                expectations.append(action)

        # Reporting requirements (any team member)
        if any(kw in line_lower for kw in reporting_keywords):
            if len(line.split()) >= 4:
                reporting.append(line)

    # Team dependencies: items in action sections owned by others that block Hugh
    dependency_keywords = ["waiting", "depends on", "need from", "blocked by", "pending"]
    text_lower = raw_text.lower()
    for kw in dependency_keywords:
        if kw in text_lower:
            idx = text_lower.find(kw)
            start = max(0, idx - 60)
            end = min(len(raw_text), idx + 100)
            snippet = raw_text[start:end].replace("\n", " ").strip()
            if snippet and snippet not in dependencies:
                dependencies.append(snippet)

    # Deduplicate
    seen: set = set()
    expectations = [e for e in expectations if not (e in seen or seen.add(e))]  # type: ignore
    seen = set()
    reporting = [r for r in reporting if not (r in seen or seen.add(r))]  # type: ignore

    return {
        "expectations_on_hugh": expectations,
        "team_dependencies": dependencies,
        "reporting_requirements": reporting,
    }


# ---------------------------------------------------------------------------
# Step 6 — Follow-up gap detection
# ---------------------------------------------------------------------------

def detect_follow_up_gaps(
    commitments_gaps: list,
    meeting_type: str,
    sections: dict,
) -> list:
    """
    Combine gaps from commitment extraction with structural gaps:
    - Next steps section exists but is empty
    - No Hugh commitments in an external meeting (suspicious)
    - Discussion mentions topics with no corresponding action item
    """
    gaps = list(commitments_gaps)  # start with gaps already found

    next_step_lines = [
        l for l in sections.get("next_steps", []) + sections.get("action_items", [])
        if l.strip()
    ]
    if not next_step_lines:
        gaps.append({
            "context": "No action items or next steps section found",
            "issue": "no next step",
            "recommended_action": "Add a Next Steps section to this note and confirm owners.",
        })

    return gaps


# ---------------------------------------------------------------------------
# Step 7 — Meeting summary (derived, not AI-generated)
# ---------------------------------------------------------------------------

def build_meeting_summary(
    title: str,
    date: Optional[str],
    meeting_type: str,
    hugh_owns: list,
    others_owe: list,
    revenue_signals: list,
) -> dict:
    """
    Build a short human-readable summary from extracted data.
    All three fields are derived from structured output — no generative AI.
    """
    what_happened = f"{title or 'Meeting'}"
    if date:
        what_happened += f" on {date}"
    what_happened += f" ({meeting_type})."

    # Why it matters
    if meeting_type == "external":
        positive = [s for s in revenue_signals if s.get("signal")]
        risks = [s for s in revenue_signals if s.get("risk")]
        if positive and not risks:
            why = "Positive buying signals present. Deal is progressing."
        elif risks and not positive:
            why = f"Risk indicators found: {risks[0]['risk'][:80]}."
        elif positive and risks:
            why = "Mixed signals — momentum present but blockers exist."
        else:
            why = "No strong signals detected. Standard follow-up warranted."
    else:
        why = (
            f"{len(hugh_owns)} task(s) on Hugh from this meeting."
            if hugh_owns
            else "Internal alignment meeting. Check expectations and reporting requirements."
        )

    # What to do next — tasks with explicit deadlines come first
    if hugh_owns:
        prioritized = sorted(hugh_owns, key=lambda t: (t.get("due") is None))
        first = prioritized[0]
        due_str = f" (due: {first['due']})" if first.get("due") else ""
        what_next = f"{first['task']}{due_str}"
    elif others_owe:
        first = others_owe[0]
        what_next = f"Follow up with {first['owner']} on: {first['task']}"
    else:
        what_next = "No clear next action extracted. Review note manually."

    return {
        "what_happened": what_happened,
        "why_it_matters": why,
        "what_hugh_should_do_next": what_next,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process(text: str) -> dict:
    """
    Parse raw meeting note text into structured action data.
    This is the only public interface for this module.
    """
    text = clean_text(text)
    sections = detect_sections(text)

    # Meeting info
    header_lines = [l.strip() for l in sections.get("header", []) if l.strip()]
    raw_title = header_lines[0] if header_lines else ""
    raw_title = re.sub(r"^\#{1,6}\s*", "", raw_title)
    raw_title = re.sub(r"\*{1,2}(.*?)\*{1,2}", r"\1", raw_title).strip()
    title = raw_title if len(raw_title) < 200 else None

    date = extract_date("\n".join(header_lines[:5])) or extract_date(text[:400])
    meeting_type = detect_meeting_type(sections, text)

    # Commitments
    hugh_owns, others_owe, commitment_gaps = extract_commitments(sections)

    # Type-specific extraction
    if meeting_type == "external":
        revenue_signals = extract_revenue_signals(sections, text, title or "")
        internal_alignment = {
            "expectations_on_hugh": [],
            "team_dependencies": [],
            "reporting_requirements": [],
        }
    else:
        revenue_signals = []
        internal_alignment = extract_internal_alignment(sections, text)

    # Follow-up gaps
    follow_up_gaps = detect_follow_up_gaps(commitment_gaps, meeting_type, sections)

    # Summary
    meeting_summary = build_meeting_summary(
        title, date, meeting_type, hugh_owns, others_owe, revenue_signals
    )

    # Missing fields
    missing = []
    if not date:
        missing.append("meeting_info.date")
    if not title:
        missing.append("meeting_info.title")
    if not hugh_owns and not others_owe:
        missing.append("commitments (none extracted)")

    # Confidence: simple heuristic
    score = 0.0
    if title:
        score += 0.2
    if date:
        score += 0.2
    if hugh_owns:
        score += 0.4
    elif others_owe:
        score += 0.2
    if revenue_signals or internal_alignment.get("expectations_on_hugh"):
        score += 0.2

    return {
        "meeting_info": {
            "title": title,
            "date": date,
            "meeting_type": meeting_type,
        },
        "commitments": {
            "hugh_owns": hugh_owns,
            "others_owe_hugh": others_owe,
        },
        "revenue_signals": revenue_signals,
        "follow_up_gaps": follow_up_gaps,
        "internal_alignment": internal_alignment,
        "meeting_summary": meeting_summary,
        "missing_fields": missing,
        "extraction_confidence": round(score, 2),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    cli = argparse.ArgumentParser(
        description="Parse a meeting note into structured action data."
    )
    cli.add_argument("input_file", help="Path to .txt meeting note")
    cli.add_argument("--compact", action="store_true", help="Compact JSON output")
    args = cli.parse_args()

    path = Path(args.input_file)
    if not path.exists():
        print(f"Error: not found: {path}", file=sys.stderr)
        sys.exit(1)

    result = process(path.read_text(encoding="utf-8", errors="replace"))
    print(json.dumps(result, indent=None if args.compact else 2, ensure_ascii=False))
