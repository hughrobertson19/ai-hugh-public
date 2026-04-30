"""
Role-based classifier for CDP outreach.

Reads a job title string, returns one of five bucket names. Deterministic
keyword match, no LLM. First matching bucket in the cascade wins.

Functions:
  classify(title)                    -> str
  classify_file(path)                -> {bucket: count}
  load_template(bucket, campaign)    -> (subject, body)
"""

import csv
import os
import re
from datetime import date

# Cascade — order matters. First regex that matches the title wins.
# Each pattern is case-insensitive; compile once at import time.
BUCKETS = [
    ("sustainability",       r"sustainab|esg|climate|environmental affairs|carbon"),
    ("ehs",                  r"environ.*health.*safety|\behs\b|environ compliance"),
    ("supply_chain_finance", r"supply chain finance|finance.*supply chain|purchasing|procurement"),
    ("finance_exec",         r"\bcfo\b|controller|\bfinance\b"),
]
_COMPILED = [(name, re.compile(pat, re.IGNORECASE)) for name, pat in BUCKETS]


def classify(title: str) -> str:
    """Return the bucket name for a single job title. 'other' if no match."""
    t = title or ""
    for name, rx in _COMPILED:
        if rx.search(t):
            return name
    return "other"


def classify_file(input_csv: str) -> dict:
    """Run classify() over every row's title column. Read-only, no side effects.
    Returns {bucket_name: count}. Missing/empty titles count as 'other'."""
    counts = {name: 0 for name, _ in BUCKETS}
    counts["other"] = 0
    with open(input_csv, newline="") as f:
        for row in csv.DictReader(f):
            bucket = classify(row.get("title", ""))
            counts[bucket] += 1
    return counts


TEMPLATE_DIR = "templates"
DEFAULT_CAMPAIGN = "cdp_spring_2026"


def load_template(bucket: str, campaign: str = DEFAULT_CAMPAIGN) -> tuple[str, str]:
    """Read templates/<campaign>/<bucket>.txt, return (subject, body).

    File format — two sections separated by a blank line:
        SUBJECT: <subject line>
        <blank>
        <body lines...>

    Raises FileNotFoundError if the template file doesn't exist,
    ValueError if the first line is not a SUBJECT: line.
    """
    path = os.path.join(TEMPLATE_DIR, campaign, f"{bucket}.txt")
    with open(path) as f:
        text = f.read()
    # Split on the first blank line — the rest is body (preserves internal blank lines).
    head, _, body = text.partition("\n\n")
    head = head.strip()
    if not head.upper().startswith("SUBJECT:"):
        raise ValueError(f"{path}: first line must be 'SUBJECT: ...' (got {head!r})")
    subject = head.split(":", 1)[1].strip()
    return subject, body.rstrip("\n")


def parse_first_name(name: str) -> str:
    """Return the first token of `name` if it has 2+ tokens, else ''.

    1-token rows are ambiguous (bare last name, initial+surname, concatenated
    first+last) — safer to render empty than to guess wrong.
    """
    tokens = (name or "").strip().split()
    return tokens[0] if len(tokens) >= 2 else ""


def render_email(subject: str, body: str, first_name: str, company: str) -> tuple[str, str]:
    """Apply .format(first_name=..., company=...) to subject and body.

    Falls back to the unrendered string if .format() raises — templates today
    have no placeholders, and a stray brace shouldn't crash the batch.
    """
    try:
        rendered_subject = subject.format(first_name=first_name, company=company)
    except (KeyError, IndexError, ValueError):
        rendered_subject = subject
    try:
        rendered_body = body.format(first_name=first_name, company=company)
    except (KeyError, IndexError, ValueError):
        rendered_body = body
    return rendered_subject, rendered_body


def render_batch(input_csv: str, output_csv: str, campaign: str = DEFAULT_CAMPAIGN) -> dict:
    """Render one email per lead and write a CSV in send_emails.py's schema.

    Columns: first_name, company, email, subject, body.
    Skips rows with missing/blank email. Returns per-bucket counts +
    'skipped_no_email'.
    """
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    # Cache loaded templates so we read each file once per run.
    template_cache: dict[str, tuple[str, str]] = {}

    def get_template(bucket: str) -> tuple[str, str]:
        if bucket not in template_cache:
            template_cache[bucket] = load_template(bucket, campaign)
        return template_cache[bucket]

    counts = {name: 0 for name, _ in BUCKETS}
    counts["other"] = 0
    counts["skipped_no_email"] = 0

    with open(input_csv, newline="") as fin, open(output_csv, "w", newline="") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(
            fout, fieldnames=["first_name", "company", "email", "subject", "body"]
        )
        writer.writeheader()

        for row in reader:
            email = (row.get("email") or "").strip()
            if not email:
                counts["skipped_no_email"] += 1
                continue

            title = row.get("title") or ""
            bucket = classify(title)
            subject, body = get_template(bucket)
            first_name = parse_first_name(row.get("name") or "")
            company = (row.get("company") or "").strip()
            rendered_subject, rendered_body = render_email(subject, body, first_name, company)

            writer.writerow({
                "first_name": first_name,
                "company": company,
                "email": email,
                "subject": rendered_subject,
                "body": rendered_body,
            })
            counts[bucket] += 1

    return counts


if __name__ == "__main__":
    PATH = "intel/example_campaign/leads_with_titles.csv"
    counts = classify_file(PATH)
    total = sum(counts.values())

    # Collect 3 sample titles per bucket for eyeball QA. Separate pass so
    # classify_file stays minimal (just returns counts).
    samples = {name: [] for name in counts}
    with open(PATH, newline="") as f:
        for row in csv.DictReader(f):
            title = (row.get("title") or "").strip()
            if not title:
                continue
            b = classify(title)
            if len(samples[b]) < 3:
                samples[b].append(title)

    print(f"=== Classification — {PATH} ===")
    print(f"Total rows: {total}\n")

    # Print in cascade order so priority is visible, then 'other' last.
    for name, _ in BUCKETS + [("other", None)]:
        n = counts[name]
        pct = (n / total * 100) if total else 0
        print(f"{name:<22} {n:>4}   ({pct:5.1f}%)")
        for t in samples[name]:
            print(f"    • {t}")
        print()

    # Template loader sanity check — graceful on missing files so this
    # section doesn't crash before the templates have been created.
    print("=== Template loader sanity check ===")
    for name, _ in BUCKETS + [("other", None)]:
        try:
            subj, _body = load_template(name)
            print(f"  {name:<22} SUBJECT: {subj}")
        except FileNotFoundError:
            print(f"  {name:<22} (template not yet created)")
        except ValueError as e:
            print(f"  {name:<22} MALFORMED: {e}")

    # Render the per-lead CSV in send_emails.py's schema.
    out_path = f"intel/example_campaign/rendered/cdp_spring_2026_{date.today().isoformat()}.csv"
    print(f"\n=== Rendering batch → {out_path} ===")
    stats = render_batch(PATH, out_path)
    print(stats)
