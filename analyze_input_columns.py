import csv
from collections import Counter
from pathlib import Path

INPUT_DIR = Path("input")
MAX_VALUES = 30
EMPTY_TOKEN = "<EMPTY>"


def normalize_header(name: str) -> str:
    # Strip UTF-8 BOM if present
    return name.lstrip("\ufeff")


def analyze_file(path: Path) -> None:
    print(f"File: {path}")
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print("  (no header)")
            return
        fieldnames = [normalize_header(n) for n in reader.fieldnames]
        # Ensure DictReader uses normalized headers (e.g., strip BOM)
        reader.fieldnames = fieldnames
        counters = {name: Counter() for name in fieldnames}
        rows = 0
        for row in reader:
            rows += 1
            for name in fieldnames:
                value = row.get(name, "")
                if value is None or str(value).strip() == "":
                    counters[name][EMPTY_TOKEN] += 1
                else:
                    counters[name][str(value)] += 1

    print(f"  Rows: {rows}")
    for name in fieldnames:
        counter = counters[name]
        unique_count = len(counter)
        print(f"  Column: {name}")
        print(f"    Unique values: {unique_count}")
        if unique_count == 0:
            continue
        if unique_count <= MAX_VALUES:
            items = counter.most_common()
        else:
            items = counter.most_common(MAX_VALUES)
        for value, count in items:
            print(f"    - {value} ({count})")
        if unique_count > MAX_VALUES:
            other = unique_count - MAX_VALUES
            print(f"    ... and {other} more")
    print()


def main() -> None:
    if not INPUT_DIR.exists():
        raise SystemExit(f"Input directory not found: {INPUT_DIR}")

    files = sorted(
        p for p in INPUT_DIR.iterdir() if p.is_file() and p.suffix == ".csv"
    )
    if not files:
        raise SystemExit(f"No CSV files found in: {INPUT_DIR}")

    for path in files:
        analyze_file(path)


if __name__ == "__main__":
    main()
