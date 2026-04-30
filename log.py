from __future__ import annotations
"""
log.py — AI Hugh Outcome Logger (fast CLI)

Quick usage:
    python3 log.py EVT-123 "Siegwerk GmbH" positive_reply 2
    python3 log.py EVT-999 "Acme Corp" no_response

Views:
    python3 log.py --pending              Show actions awaiting outcomes
    python3 log.py --view "Siegwerk GmbH" Account quick view
    python3 log.py --analytics            Conversion rates & effectiveness
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "workflows"))

from pending_actions import (
    VALID_OUTCOMES, get_pending, is_known_event,
    is_already_resolved, resolve_action,
)
from outcome_store import OutcomeEvent, log_outcome, get_account_outcomes


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _print_valid_outcomes():
    print("\nAllowed outcomes:")
    for o in sorted(VALID_OUTCOMES):
        print(f"  - {o}")


def _print_usage():
    print("Usage:")
    print("  python3 log.py EVT-xxx \"Account\" outcome [days]")
    print("  python3 log.py --pending")
    print("  python3 log.py --view \"Account\"")
    print("  python3 log.py --analytics")
    _print_valid_outcomes()


# ─────────────────────────────────────────────
# --pending
# ─────────────────────────────────────────────

def cmd_pending():
    pending = get_pending()
    if not pending:
        print("\nNo pending actions. All outcomes logged.")
        return

    print(f"\n=== PENDING ACTIONS ({len(pending)} awaiting outcome) ===\n")
    for i, p in enumerate(pending, 1):
        ts = p.get("timestamp", "")[:16]
        channel = p.get("recommended_channel", "—")
        action = p.get("action_type", "") or p.get("motion", "") or "—"
        signal = p.get("signal", "")
        print(f"  {i}. {p['event_id']}  {p.get('account', '?')}")
        print(f"     action: {action}  |  channel: {channel}  |  score: {p.get('score', '—')}")
        if signal:
            print(f"     signal: {signal[:60]}")
        print(f"     generated: {ts}")
        print()

    print("Log an outcome:")
    print("  python3 log.py <event_id> \"<account>\" <outcome> [days]")
    _print_valid_outcomes()


# ─────────────────────────────────────────────
# --view
# ─────────────────────────────────────────────

def cmd_view(account: str):
    outcomes = get_account_outcomes(account)
    if not outcomes:
        print(f"\nNo data for '{account}'")
        return

    # Count by outcome type
    counts = {}
    for o in outcomes:
        oc = o.get("outcome", "unknown")
        counts[oc] = counts.get(oc, 0) + 1

    total = len(outcomes)
    positive = counts.get("positive_reply", 0) + counts.get("meeting_booked", 0)
    conversion = f"{positive/total:.0%}" if total > 0 else "—"

    print(f"\n=== {account} — {total} events ===\n")

    # Summary
    print(f"  Conversion: {conversion} ({positive}/{total} positive)")
    print(f"  Breakdown:  ", end="")
    print("  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print()

    # Recent actions (last 10)
    print("  Recent:")
    for e in outcomes[-10:]:
        ts = e.get("timestamp", "")[:16]
        oc = e.get("outcome", "?")
        action = e.get("action_type", "") or e.get("motion", "") or "—"
        days = e.get("response_time_days")
        days_str = f" ({days}d)" if days is not None else ""
        print(f"    {e.get('event_id', '?')}  {oc}{days_str}  [{action}]  {ts}")


# ─────────────────────────────────────────────
# --analytics
# ─────────────────────────────────────────────

def cmd_analytics():
    from outcome_analytics import (
        get_conversion_rates, motion_effectiveness,
        signal_effectiveness, time_to_response_analysis,
    )

    print("\n=== CONVERSION RATES BY ACTION TYPE ===\n")
    rates = get_conversion_rates()
    if not rates:
        print("  No outcome data yet.")
    for action, data in sorted(rates.items()):
        print(f"  {action}: {data['sent']} sent -> {data['replied']} replied -> {data['meetings']} meetings | rate={data['conversion_rate']:.0%}")

    print("\n=== MOTION EFFECTIVENESS ===\n")
    motions = motion_effectiveness()
    if not motions:
        print("  No data yet.")
    for motion, data in sorted(motions.items()):
        print(f"  {motion}: {data['total']} total -> {data['replied']} replied -> {data['meetings']} meetings | rate={data['conversion_rate']:.0%}")

    print("\n=== SIGNAL EFFECTIVENESS ===\n")
    signals = signal_effectiveness()
    if not signals:
        print("  No data yet.")
    for signal, data in sorted(signals.items(), key=lambda x: x[1]["conversion_rate"], reverse=True):
        print(f"  {signal[:50]}: {data['total']} total -> rate={data['conversion_rate']:.0%}")

    print("\n=== TIME TO RESPONSE ===\n")
    ttr = time_to_response_analysis()
    if not ttr:
        print("  No response time data yet.")
    for action, data in sorted(ttr.items()):
        print(f"  {action}: avg={data['avg_days']}d | min={data['min_days']}d | max={data['max_days']}d ({data['count']} responses)")

    print()


# ─────────────────────────────────────────────
# LOG COMMAND (positional args)
# ─────────────────────────────────────────────

def cmd_log(event_id: str, account: str, outcome: str, days: int | None):
    # ── Validate outcome ──
    if outcome not in VALID_OUTCOMES:
        print(f"\n[error] Invalid outcome: '{outcome}'")
        _print_valid_outcomes()
        sys.exit(1)

    # ── Validate event_id format ──
    if not event_id.startswith("EVT-"):
        print(f"\n[error] Invalid event_id: '{event_id}'")
        print("  Event IDs start with EVT- (e.g. EVT-56b3be8c08)")
        sys.exit(1)

    # ── Check event_id exists in registry ──
    if not is_known_event(event_id):
        print(f"\n[error] Unknown event_id: {event_id}")
        print("  This event was not generated by the action engine.")
        print("  Run: python3 log.py --pending  to see valid events")
        sys.exit(1)

    # ── Duplicate protection ──
    if is_already_resolved(event_id):
        print(f"\n[blocked] {event_id} already has an outcome logged.")
        print("  Duplicate logging is not allowed.")
        sys.exit(1)

    # ── Log to outcome store ──
    event = OutcomeEvent(
        event_id=event_id,
        account=account,
        outcome=outcome,
        response_time_days=days,
    )
    log_outcome(event)

    # ── Mark resolved in pending registry ──
    resolve_action(event_id, outcome)

    # ── Confirmation ──
    days_str = f" -- {days} days" if days is not None else ""
    print(f"\n[logged] {event_id} -- {account} -- {outcome}{days_str}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if not args or args[0] in ("-h", "--help"):
        _print_usage()
        return

    # ── Flag commands ──
    if args[0] == "--pending":
        cmd_pending()
        return

    if args[0] == "--view":
        if len(args) < 2:
            print("[error] --view requires an account name")
            print("  python3 log.py --view \"Siegwerk GmbH\"")
            sys.exit(1)
        cmd_view(args[1])
        return

    if args[0] == "--analytics":
        cmd_analytics()
        return

    # ── Positional log command ──
    if len(args) < 3:
        print("[error] Not enough arguments.")
        _print_usage()
        sys.exit(1)

    event_id = args[0]
    account = args[1]
    outcome = args[2]
    days = None
    if len(args) >= 4:
        try:
            days = int(args[3])
        except ValueError:
            print(f"[error] Days must be a number, got: '{args[3]}'")
            sys.exit(1)

    cmd_log(event_id, account, outcome, days)


if __name__ == "__main__":
    main()
