#!/usr/bin/env python3
"""
Render today_calls.csv as a per-lead call sheet (live script + voicemail),
matching the format of call_sheet_today_send_audited_with_copy.txt.

Sort preserved from CSV: ET → AT → CT → MT → PT → unknown,
warmest (Apr 21) first within each TZ.
"""

import csv
import os

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "intel", "example_campaign"))
IN_CSV = os.path.join(BASE, "today_calls.csv")
OUT_TXT = os.path.join(BASE, "call_sheet_today.txt")

SEP_EQ = "=" * 44
SEP_DASH = "-" * 44


def block(r):
    first = (r["name"].split() or [r["email"]])[0]
    company = r["company"] or "—"
    tz = r["timezone"] or "??"
    phone = r["phone"].strip() or "N/A"
    mobile = r["mobile"].strip() or "N/A"
    emailed = r["emailed"]
    emailed_label = {
        "2026-04-21": "emailed Apr 21",
        "2026-04-22": "emailed Apr 22",
        "2026-04-23": "emailed today",
    }.get(emailed, f"emailed {emailed}")

    lines = []
    lines.append(SEP_EQ)
    lines.append(f"{first} — {company}  [TZ: {tz}]  ({emailed_label})")
    lines.append(f"Phone: {phone}  |  Mobile: {mobile}")
    lines.append(SEP_EQ)
    lines.append("")
    lines.append("LIVE CALL SCRIPT:")
    lines.append(f'"Hey {first}, it\'s {{Sender Name}} from {{Your Company}}, how\'s it going?')
    lines.append("")
    lines.append("[pause]")
    lines.append("")
    lines.append("Reason I'm calling — {trigger event for the campaign}")
    lines.append("and we just launched {one-sentence product hook}.")
    lines.append("")
    lines.append(f"Was curious how {company} is handling {{the relevant workflow}}")
    lines.append('this year?"')
    lines.append("")
    lines.append('[If interested] → "Worth a 15-minute call this week')
    lines.append('to walk through it?"')
    lines.append("")
    lines.append('[If not the right person] → "No worries — who owns')
    lines.append('{the relevant workflow} on your end?"')
    lines.append("")
    lines.append('[If busy] → "Totally get it — I just sent you an email')
    lines.append('with the details. Worth a look when you get a sec."')
    lines.append("")
    lines.append(SEP_DASH)
    lines.append("")
    lines.append("VOICEMAIL SCRIPT:")
    lines.append(f'"Hey {first}, it\'s {{Sender Name}} from {{Your Company}}. Just sent you')
    lines.append("an email about {topic} — worth a quick look.")
    lines.append('Cheers."')
    lines.append("")
    lines.append(SEP_EQ)
    return "\n".join(lines)


def main():
    with open(IN_CSV, newline="") as f:
        rows = list(csv.DictReader(f))

    counts = {"2026-04-21": 0, "2026-04-22": 0, "2026-04-23": 0}
    tz_counts = {}
    for r in rows:
        counts[r["emailed"]] = counts.get(r["emailed"], 0) + 1
        tz_counts[r["timezone"]] = tz_counts.get(r["timezone"], 0) + 1

    header = [
        f"=== CALL SHEET — 2026-04-23 ({len(rows)} leads) ===",
        "Source: today_calls.csv",
        f"Breakdown: Apr 21 warm={counts.get('2026-04-21',0)} · "
        f"Apr 22 warm={counts.get('2026-04-22',0)} · "
        f"today={counts.get('2026-04-23',0)}",
        "Timezones: " + " · ".join(f"{tz}={n}" for tz, n in sorted(tz_counts.items())),
        "Call order: ET → AT → CT, warmest (Apr 21) first within each TZ",
        "",
    ]

    body = "\n\n\n".join(block(r) for r in rows)
    with open(OUT_TXT, "w") as f:
        f.write("\n".join(header))
        f.write("\n\n")
        f.write(body)
        f.write("\n")

    print(f"Wrote {len(rows)} call blocks to {OUT_TXT}")
    print(f"Breakdown: Apr 21={counts.get('2026-04-21',0)} · Apr 22={counts.get('2026-04-22',0)} · today={counts.get('2026-04-23',0)}")


if __name__ == "__main__":
    main()
