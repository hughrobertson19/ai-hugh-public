"""
Screenshot Watcher — AI Hugh
Watches a folder for new screenshots and processes them through the intake pipeline.

Usage:
  python3 screenshot_watcher.py

Drop a .png, .jpg, .jpeg, or .webp into the screenshots/ folder.
The watcher will process it and print the structured result.

Ctrl+C to stop.
"""

import sys
import time
from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Setup ────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent
WATCH_FOLDER = PROJECT_ROOT / "screenshots"
ALLOWED_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}

sys.path.insert(0, str(PROJECT_ROOT / "workflows"))


# ── Handler ──────────────────────────────────────────────────────────────────

class ScreenshotHandler(FileSystemEventHandler):
    def on_created(self, event):
        # Ignore directories
        if event.is_directory:
            return

        path = Path(event.src_path)

        # Ignore non-image files
        if path.suffix.lower() not in ALLOWED_EXTENSIONS:
            print(f"[watcher] Ignored: {path.name} (not an image)")
            return

        # Small delay — file may still be writing
        time.sleep(0.5)

        print(f"\n[watcher] New screenshot: {path.name}")
        print(f"[watcher] Processing...")

        try:
            from screenshot_intake import process_screenshot

            result = process_screenshot(str(path))

            print(f"[watcher] Extraction confidence: {result.normalized['extraction_confidence']}")
            print(f"[watcher] Context quality:       {result.interpretation.context_quality}")
            print(f"[watcher] Sufficient context:    {result.interpretation.has_sufficient_context}")
            print(f"[watcher] Inferred trigger:      {result.interpretation.inferred_trigger}")
            print(f"[watcher] Inferred warmth:       {result.interpretation.inferred_warmth}")

            if result.interpretation.warnings:
                print(f"[watcher] Warnings:")
                for w in result.interpretation.warnings:
                    print(f"           {w}")

            if result.intake:
                print(f"\n[watcher] === SDR INTAKE ===")
                print(f"  account:  {result.intake.account_name}")
                print(f"  contact:  {result.intake.contact_name}")
                print(f"  title:    {result.intake.contact_title}")
                print(f"  trigger:  {result.intake.trigger_type}")
                print(f"  warmth:   {result.intake.relationship_warmth}")
                print(f"  product:  {result.intake.product_family}")

                # Show routing decision
                try:
                    from outreach_router import route_scenario
                    from account_memory import load_account, append_event, create_event
                    from deal_state_engine import compute_deal_state
                    from action_engine import run_action_engine

                    route = route_scenario(**result.intake.to_router_kwargs())
                    print(f"\n[watcher] === ROUTING ===")
                    print(f"  persona:    {route.persona}")
                    print(f"  trigger:    {route.trigger_type}")
                    print(f"  confidence: {route.confidence_level}")

                    # Load account memory + append this event
                    account_name = result.intake.account_name
                    action_data = {
                        "account": account_name,
                        "contact": result.intake.contact_name,
                        "title": result.intake.contact_title,
                        "trigger": route.trigger_type,
                        "warmth": route.relationship_warmth,
                        "product": result.intake.product_family,
                        "persona": route.persona,
                        "confidence": route.confidence_level,
                    }

                    # Append event to persistent memory
                    event = create_event(
                        trigger=route.trigger_type,
                        warmth=route.relationship_warmth,
                        persona=route.persona,
                        action_type="pending",  # will be updated after action engine
                        confidence=route.confidence_level,
                        raw_summary=f"{result.intake.what_changed[:80]}" if result.intake.what_changed else "screenshot intake",
                    )
                    history = append_event(account_name, event)

                    # Compute deal state from history
                    deal_state = compute_deal_state(history)
                    print(f"\n[watcher] === DEAL STATE ({history.event_count} events) ===")
                    print(deal_state.to_display())

                    # Run action engine WITH deal state
                    action = run_action_engine(action_data, deal_state=deal_state)
                    print(f"\n[watcher] === ACTION ENGINE ===")
                    print(action.to_display())

                    # Update the last event with the actual action type
                    if history.events:
                        history.events[-1]["action_type"] = action.action_type
                        from account_memory import _save_account
                        _save_account(history)

                except Exception as e:
                    print(f"[watcher] Routing/action skipped: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"\n[watcher] Insufficient context — cannot generate outreach.")

            print(f"\n[watcher] Done. Waiting for next screenshot...\n")

        except Exception as e:
            print(f"[watcher] ERROR processing {path.name}: {e}")
            print(f"[watcher] Continuing to watch...\n")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not WATCH_FOLDER.exists():
        WATCH_FOLDER.mkdir(parents=True)
        print(f"[watcher] Created folder: {WATCH_FOLDER}")

    print(f"[watcher] Watching: {WATCH_FOLDER}")
    print(f"[watcher] Drop a screenshot (.png, .jpg, .jpeg, .webp) to process it.")
    print(f"[watcher] Ctrl+C to stop.\n")

    observer = Observer()
    observer.schedule(ScreenshotHandler(), str(WATCH_FOLDER), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[watcher] Stopping...")
        observer.stop()

    observer.join()
    print("[watcher] Stopped.")
