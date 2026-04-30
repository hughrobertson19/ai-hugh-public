from pathlib import Path
import json
import re

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "intel/example_campaign/messaging/4.22_Sales_Development_Support_Package.csv"
OUTPUT_DIR = BASE_DIR / "intel/example_campaign/output/sales_playbooks"
KNOWLEDGE_BRIEFS_DIR = BASE_DIR / "intel/example_campaign/knowledge_briefs"


REQUIRED_COLUMNS = ["cou", "sub_cou", "action", "materials"]


def summarize_items(items: list[str]) -> list[str]:
    seen = []
    for item in items:
        cleaned = clean_text(item)
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return seen


def build_markdown_brief(sub_cou: str, records: list[dict]) -> str:
    actions = summarize_items([record["action"] for record in records])
    materials = summarize_items([record["materials"] for record in records])
    cou_values = summarize_items([record["cou"] for record in records])

    top_materials = materials[:8]

    lines = [
        f"# {sub_cou} Sales Playbook Brief",
        "",
        "## Overview",
        f"- COU: {', '.join(cou_values) if cou_values else 'Unknown'}",
        f"- Sub COU: {sub_cou}",
        f"- Record count: {len(records)}",
        "",
        "## Actions Covered",
    ]

    if actions:
        for action in actions:
            lines.append(f"- {action}")
    else:
        lines.append("- No actions found")

    lines.extend([
        "",
        "## Core Messaging and Materials",
    ])

    if top_materials:
        for item in top_materials:
            bullet_text = item.replace("\n", " ").strip()
            lines.append(f"- {bullet_text}")
    else:
        lines.append("- No materials found")

    lines.extend([
        "",
        "## Likely Knowledge Still Needed",
        "- Buyer personas and economic buyers",
        "- Buying triggers and timing signals",
        "- Objections and objection handling",
        "- Competitor landscape",
        "- Proof points, case studies, and measurable outcomes",
        "- Implementation and integration realities",
        "",
    ])

    return "\n".join(lines)


def slugify(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unknown"


def clean_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_BRIEFS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(INPUT_FILE)
    df.columns = [slugify(col) for col in df.columns]

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found columns: {list(df.columns)}")

    for column in REQUIRED_COLUMNS:
        df[column] = df[column].apply(clean_text)

    df = df[(df["sub_cou"] != "") & (df["materials"] != "")].copy()

    grouped = df.groupby("sub_cou", dropna=False)

    manifest = []

    for sub_cou, group in grouped:
        records = []
        for _, row in group.iterrows():
            records.append(
                {
                    "cou": row["cou"],
                    "sub_cou": row["sub_cou"],
                    "action": row["action"],
                    "materials": row["materials"],
                }
            )

        filename = f"{slugify(sub_cou)}_playbook.json"
        output_path = OUTPUT_DIR / filename

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        brief_filename = f"{slugify(sub_cou)}_sales_playbook.md"
        brief_path = KNOWLEDGE_BRIEFS_DIR / brief_filename
        brief_content = build_markdown_brief(sub_cou, records)

        with brief_path.open("w", encoding="utf-8") as f:
            f.write(brief_content)

        manifest.append(
            {
                "sub_cou": sub_cou,
                "record_count": len(records),
                "file": str(output_path.relative_to(BASE_DIR)),
            }
        )

        print(f"Saved {len(records)} records -> {output_path.relative_to(BASE_DIR)}")
        print(f"Saved brief -> {brief_path.relative_to(BASE_DIR)}")

    manifest_path = OUTPUT_DIR / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"\nCreated {len(manifest)} playbook files.")
    print(f"Created {len(manifest)} knowledge briefs.")
    print(f"Manifest saved -> {manifest_path.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()