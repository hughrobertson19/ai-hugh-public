#!/usr/bin/env python3
"""
Outreach Runner
One lead at a time. Email + call + WhatsApp ready to copy.
Press Enter to advance. Type 'q' to quit. Type 'skip' to skip.
"""

import csv
import os
import re

# --- Config ---
LEADS_FILE = "intel/example_campaign/today_send_86.csv"
TITLES_FILE = "intel/example_campaign/leads_with_titles.csv"
SIGNATURE = """Best regards,
{Sender Name}
{Title}
{Team or Department}
{Your Company} | {City, State}
+1 (000) 000-0000
your-email@company.com"""

# Known last.first email domains
LASTFIRST_DOMAINS = {"example-domain-1.example.com", "example-domain-2.example.com"}

# --- Email templates ---
# Replace these with your campaign copy. Keep the structure: a tight subject,
# a 2–3 paragraph body that names a specific pain, a one-sentence product hook,
# and a single CTA.
COLD_SUBJECT = "{Subject line for the current campaign}"
COLD_BODY = """{Opening paragraph naming the timing or trigger event that makes this outreach relevant right now.}

{Middle paragraph describing your product's unique angle in one or two sentences. Avoid feature lists; lead with the differentiator.}

{Closing paragraph with a single, low-friction CTA — e.g., a 15-minute walkthrough.}"""

WARM_SUBJECT = "{Subject line for warm/partner accounts}"
WARM_BODY = """{Opening that acknowledges the existing relationship in one sentence.}

{Middle paragraph framing the new offering as an extension of what they already use, not a rip-and-replace.}

{Closing CTA — even shorter than the cold version, since they already know you.}"""

CALL_SCRIPT = """G'day {first}, this is {Sender Name}. I just sent you an email about {topic} — wanted to put a voice to it real quick.

{One-sentence product hook.} With {timing trigger} coming up, would a 15-minute walkthrough be worth it before the window closes?"""

VOICEMAIL_SCRIPT = """G'day {first}, {Sender Name}. I just shot you an email about {topic}. Take a look when you get a chance — happy to do a quick walkthrough if it's relevant. +1 (000) 000-0000."""

WHATSAPP_MSG = """Hi {first}, {Sender Name}. {One-sentence product hook.} With {timing trigger} coming up, would a quick 15-min demo be useful before the window opens?"""


def clear():
    os.system("clear" if os.name != "nt" else "cls")


def get_first_name(name, email):
    """Extract first name, handling last.first domains."""
    domain = email.split("@")[1].lower()
    local = re.sub(r"\d+$", "", email.split("@")[0].lower())

    # Handle last.first domains
    if domain in LASTFIRST_DOMAINS and "." in local:
        parts = local.split(".")
        return parts[1].capitalize()

    # "First Last" format
    if " " in name:
        return name.split()[0]

    # Try email: first.last or first_last
    if "." in local:
        return local.split(".")[0].capitalize()
    if "_" in local:
        return local.split("_")[0].capitalize()

    return name


def is_intl_number(number):
    """Check if a phone number is international (non-US)."""
    number = number.strip()
    if number.startswith("+") and not number.startswith("+1"):
        return True
    return False


def is_hq_number(number):
    """Check if a number looks like a switchboard/toll-free."""
    clean = re.sub(r"[^\d]", "", number)
    if clean.startswith("800") or clean.startswith("855") or clean.startswith("866") or clean.startswith("877") or clean.startswith("888"):
        return True
    return False


def main():
    # Load titles
    titles = {}
    with open(TITLES_FILE, "r") as f:
        for row in csv.DictReader(f):
            titles[row["email"]] = row.get("title", "")

    # Load leads with phone data from master file
    phones = {}
    with open("intel/example_campaign/leads.csv", "r") as f:
        for row in csv.DictReader(f):
            phones[row["email"]] = {"phone": row.get("phone", ""), "mobile": row.get("mobile", "")}

    # Load today's send list
    leads = []
    with open(LEADS_FILE, "r") as f:
        for row in csv.DictReader(f):
            leads.append(row)

    total = len(leads)
    i = 0

    while i < total:
        lead = leads[i]
        email = lead["email"]
        company = lead["company"]
        first = get_first_name(lead["first_name"] if lead["first_name"] else lead.get("name", ""), email)
        title = titles.get(email, "")
        phone_data = phones.get(email, {"phone": "", "mobile": ""})
        phone = phone_data["phone"].strip()
        mobile = phone_data["mobile"].strip()

        # Determine template
        is_warm = "Partner Co" in lead.get("subject", "") or "extending" in lead.get("subject", "")
        subject = WARM_SUBJECT if is_warm else COLD_SUBJECT
        body = WARM_BODY if is_warm else COLD_BODY

        # Phone display logic
        if phone and not is_hq_number(phone):
            phone_display = phone
        elif phone:
            phone_display = f"{phone} (HQ)"
        else:
            phone_display = "--"

        mobile_display = mobile if mobile else "--"

        # WhatsApp: only for international mobiles
        show_whatsapp = mobile and is_intl_number(mobile)

        clear()
        print("=" * 60)
        print(f"  LEAD {i + 1} OF {total}")
        print("=" * 60)
        print(f"  Name:     {first} ({lead.get('first_name', '')} — full record)")
        print(f"  Company:  {company}")
        print(f"  Title:    {title}")
        print(f"  Email:    {email}")
        print(f"  Phone:    {phone_display}")
        print(f"  Mobile:   {mobile_display}")
        print("=" * 60)

        # --- EMAIL ---
        print("\n--- EMAIL ---")
        print(f"Subject: {subject}\n")
        print(f"Hi {first},\n")
        print(body)
        print(f"\n{SIGNATURE}")

        # --- CALL ---
        print("\n--- CALL SCRIPT ---")
        print(CALL_SCRIPT.format(first=first))

        # --- VOICEMAIL ---
        print("\n--- VOICEMAIL ---")
        print(VOICEMAIL_SCRIPT.format(first=first))

        # --- WHATSAPP (intl only) ---
        if show_whatsapp:
            print(f"\n--- WHATSAPP ({mobile}) ---")
            print(WHATSAPP_MSG.format(first=first))

        print("\n" + "=" * 60)
        action = input("  [Enter] next  |  [s] skip  |  [q] quit  >  ").strip().lower()

        if action == "q":
            print(f"\nStopped at lead {i + 1} of {total}. Nice work.")
            break
        elif action == "s":
            i += 1
            continue
        else:
            i += 1

    if i >= total:
        print(f"\nAll {total} leads done. Go home.")


if __name__ == "__main__":
    main()
