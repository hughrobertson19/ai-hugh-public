from datetime import datetime
from task_engine import (
    extract_meeting_signal,
    apply_meeting_outcome,
    get_daily_focus,
)

# === SIMPLE NOTE TEST RUNNER ===

def run_note_test(
    notes_path: str,
    lead_id: str
):
    # 1. Load notes
    with open(notes_path, "r") as f:
        notes = f.read()

    # 2. Extract meeting signal
    signal = extract_meeting_signal(notes)
    print("Extracted signal:", signal)

    # 3. Apply outcome to task engine
    apply_meeting_outcome(
        lead_id=lead_id,
        outcome=signal["outcome"],
        meeting_date_iso=signal["meeting_date_iso"],
        notes="Auto-applied from note test"
    )

    # 4. Ask AI Hugh what matters today
    today_iso = datetime.now().isoformat()
    focus = get_daily_focus(today_iso=today_iso)
    print("\n=== AI HUGH DAILY FOCUS ===")
    print(focus)


# === RUN TEST ===
if __name__ == "__main__":
    run_note_test(
        notes_path="test_meeting.txt",
        lead_id="ag_ul"  # must exist in tasks.json
    )