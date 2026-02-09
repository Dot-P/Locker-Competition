from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")

APP_FILE = INPUT_DIR / "applicant_data.csv"
PAR_FILE = INPUT_DIR / "partner_data.csv"

APP_COLUMNS = [
    "タイムスタンプ",
    "メールアドレス",
    "規約への同意",
    "申請者の学籍番号",
    "申請者の氏名",
    "申請者の学生証写真",
    "共同利用者の有無",
    "共同利用者の学籍番号",
    "共同利用者の氏名",
    "階数希望選択（共同利用者なし）",
    "階数希望選択（共同利用者あり）",
]

PAR_COLUMNS = [
    "タイムスタンプ",
    "メールアドレス",
    "規約への同意",
    "共同利用者の学籍番号",
    "共同利用者の氏名",
    "共同利用者の学生証写真",
]

STUDENT_ID_RE = re.compile(r"^(15\d{5}|[48][1-6]\d{5})$")


@dataclass
class Submission:
    role: str  # "app" or "par"
    row_index: int
    row: Dict[str, str]
    timestamp: datetime
    term: int
    person_id: str
    person_name: str
    photo: str
    consent: str
    partner_id: Optional[str] = None
    partner_name: Optional[str] = None
    has_partner: Optional[str] = None
    floor: Optional[int] = None
    error: Optional[str] = None


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def normalize_header(name: str) -> str:
    return name.lstrip("\ufeff")


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"Missing header: {path}")
        fieldnames = [normalize_header(n) for n in reader.fieldnames]
        reader.fieldnames = fieldnames
        rows = [row for row in reader]
    return fieldnames, rows


def is_valid_student_id(value: str) -> bool:
    return bool(STUDENT_ID_RE.match(value))


def parse_floor(row: Dict[str, str]) -> Optional[int]:
    a = row.get("階数希望選択（共同利用者なし）", "").strip()
    b = row.get("階数希望選択（共同利用者あり）", "").strip()
    if (a and b) or (not a and not b):
        return None
    value = a or b
    if value.endswith("階"):
        value = value.replace("階", "")
    try:
        floor = int(value)
    except ValueError:
        return None
    if floor < 2 or floor > 6:
        return None
    return floor


def term_index(ts: datetime, start: datetime) -> int:
    delta = ts - start
    return int(delta.total_seconds() // (7 * 24 * 60 * 60))


def term_start_from_timestamp(ts: datetime) -> datetime:
    start = datetime(ts.year, 4, 1, 0, 0, 0)
    if ts < start:
        start = datetime(ts.year - 1, 4, 1, 0, 0, 0)
    return start


def build_submissions(
    app_rows: List[Dict[str, str]],
    par_rows: List[Dict[str, str]],
    start: datetime,
) -> Tuple[List[Submission], List[Submission]]:
    apps: List[Submission] = []
    pars: List[Submission] = []

    for i, row in enumerate(app_rows):
        ts = parse_timestamp(row["タイムスタンプ"].strip())
        term = term_index(ts, start)
        sub = Submission(
            role="app",
            row_index=i,
            row=row,
            timestamp=ts,
            term=term,
            person_id=row.get("申請者の学籍番号", "").strip(),
            person_name=row.get("申請者の氏名", "").strip(),
            photo=row.get("申請者の学生証写真", "").strip(),
            consent=row.get("規約への同意", "").strip(),
            partner_id=row.get("共同利用者の学籍番号", "").strip(),
            partner_name=row.get("共同利用者の氏名", "").strip(),
            has_partner=row.get("共同利用者の有無", "").strip(),
            floor=parse_floor(row),
        )
        apps.append(sub)

    for i, row in enumerate(par_rows):
        ts = parse_timestamp(row["タイムスタンプ"].strip())
        term = term_index(ts, start)
        sub = Submission(
            role="par",
            row_index=i,
            row=row,
            timestamp=ts,
            term=term,
            person_id=row.get("共同利用者の学籍番号", "").strip(),
            person_name=row.get("共同利用者の氏名", "").strip(),
            photo=row.get("共同利用者の学生証写真", "").strip(),
            consent=row.get("規約への同意", "").strip(),
        )
        pars.append(sub)

    return apps, pars


def apply_input_validation(sub: Submission) -> Optional[str]:
    # Required fields
    if not sub.person_id or not sub.person_name:
        return "E1"
    if not is_valid_student_id(sub.person_id):
        return "E1"
    if sub.consent != "利用規約に同意する":
        return "E1"
    if sub.photo != "accept":
        return "E1"

    if sub.role == "app":
        if sub.has_partner not in (
            "共同利用者あり",
            "共同利用者なし (2階・3階のロッカーは使用できません)",
        ):
            return "E1"
        if sub.floor is None:
            return "E1"
        if sub.floor in (2, 3) and sub.has_partner != "共同利用者あり":
            return "E1"
        if sub.has_partner == "共同利用者あり":
            if not sub.partner_id or not sub.partner_name:
                return "E1"
            if not is_valid_student_id(sub.partner_id):
                return "E1"
        else:
            # no partner
            if sub.partner_id or sub.partner_name:
                return "E1"
    return None


def choose_latest_by_person(subs: List[Submission]) -> Tuple[List[Submission], List[Submission]]:
    by_person: Dict[str, List[Submission]] = {}
    for sub in subs:
        by_person.setdefault(sub.person_id, []).append(sub)

    kept: List[Submission] = []
    dropped: List[Submission] = []
    for person_id, items in by_person.items():
        items_sorted = sorted(items, key=lambda s: (s.timestamp, s.row_index))
        keep = items_sorted[-1]
        kept.append(keep)
        for s in items_sorted[:-1]:
            dropped.append(s)
    return kept, dropped


def process_term(
    term: int,
    apps: List[Submission],
    pars: List[Submission],
    ineligible_ids: set,
) -> Tuple[List[Submission], List[Submission], List[Tuple[str, str, str, str, int]]]:
    # Apply input validation
    for sub in apps + pars:
        sub.error = apply_input_validation(sub)

    # If applicant requires a partner, partner form must exist (E1)
    all_partner_ids = {p.person_id for p in pars if p.person_id}
    for app in apps:
        if app.error is None and app.has_partner == "共同利用者あり":
            if app.partner_id not in all_partner_ids:
                app.error = "E1"

    # Apply E2: ineligible (already won)
    for sub in apps + pars:
        if sub.error is None and sub.person_id in ineligible_ids:
            sub.error = "E2"

    # Only consider submissions still clean
    candidates = [s for s in apps + pars if s.error is None]

    # Rule A: same person multiple submissions -> keep latest valid
    kept, dropped = choose_latest_by_person(candidates)
    for s in dropped:
        s.error = "E3"

    candidates = [s for s in kept if s.error is None]
    app_candidates = [s for s in candidates if s.role == "app"]
    par_candidates = [s for s in candidates if s.role == "par"]

    # Build partner index (latest already enforced)
    partner_by_id: Dict[str, Submission] = {s.person_id: s for s in par_candidates}

    # Pairing and conflicts
    # Applicants needing partner must have partner submission
    for app in app_candidates:
        if app.has_partner == "共同利用者あり":
            if app.partner_id not in partner_by_id:
                app.error = "E3"

    # Resolve multiple applicants referencing the same partner
    applicants_by_partner: Dict[str, List[Submission]] = {}
    for app in app_candidates:
        if app.error is not None:
            continue
        if app.has_partner == "共同利用者あり":
            applicants_by_partner.setdefault(app.partner_id, []).append(app)

    for partner_id, apps_for_partner in applicants_by_partner.items():
        if len(apps_for_partner) <= 1:
            continue
        apps_for_partner.sort(key=lambda s: (s.timestamp, s.row_index))
        # keep earliest applicant for that partner
        keep = apps_for_partner[0]
        for s in apps_for_partner[1:]:
            s.error = "E3"

    # Determine valid pairs and mark partner status
    paired_partner_ids = set()
    valid_rows: List[Tuple[str, str, str, str, int]] = []
    for app in app_candidates:
        if app.error is not None:
            continue
        if app.has_partner == "共同利用者あり":
            partner = partner_by_id.get(app.partner_id)
            if partner is None or partner.error is not None:
                app.error = "E3"
                continue
            paired_partner_ids.add(partner.person_id)
            valid_rows.append(
                (
                    app.person_id,
                    app.person_name,
                    partner.person_id,
                    partner.person_name,
                    app.floor or 0,
                )
            )
        else:
            valid_rows.append(
                (
                    app.person_id,
                    app.person_name,
                    "",
                    "",
                    app.floor or 0,
                )
            )

    # Partners not paired are invalid (matching failed)
    for par in par_candidates:
        if par.error is None and par.person_id not in paired_partner_ids:
            par.error = "E3"

    # Set success codes
    valid_app_ids = {row[0] for row in valid_rows}
    valid_par_ids = {row[2] for row in valid_rows if row[2]}
    for sub in apps:
        if sub.error is None and sub.person_id in valid_app_ids:
            sub.error = "S0"
        elif sub.error is None:
            sub.error = "E3"
    for sub in pars:
        if sub.error is None and sub.person_id in valid_par_ids:
            sub.error = "S0"
        elif sub.error is None:
            sub.error = "E3"

    return apps, pars, valid_rows


def write_invalid(path: Path, columns: List[str], subs: List[Submission]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns + ["結果"])
        writer.writeheader()
        for sub in sorted(subs, key=lambda s: s.row_index):
            row = {k: sub.row.get(k, "") for k in columns}
            row["結果"] = sub.error or "E3"
            writer.writerow(row)


def write_valid(term_dir: Path, valid_rows: List[Tuple[str, str, str, str, int]]) -> None:
    by_floor: Dict[int, List[Tuple[str, str, str, str]]] = {i: [] for i in range(2, 7)}
    for app_id, app_name, par_id, par_name, floor in valid_rows:
        if floor in by_floor:
            by_floor[floor].append((app_id, app_name, par_id, par_name))

    for floor, rows in by_floor.items():
        rows.sort(key=lambda r: r[0])
        path = term_dir / f"valid_{floor}F.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "申請者学籍番号",
                "申請者氏名",
                "共同利用者学籍番号",
                "共同利用者氏名",
            ])
            for row in rows:
                writer.writerow(row)


def main() -> None:
    if not APP_FILE.exists() or not PAR_FILE.exists():
        raise SystemExit("Missing input files in input/")

    app_columns, app_rows = read_csv(APP_FILE)
    par_columns, par_rows = read_csv(PAR_FILE)

    # Basic header check
    for col in APP_COLUMNS:
        if col not in app_columns:
            raise SystemExit(f"Missing column in applicant CSV: {col}")
    for col in PAR_COLUMNS:
        if col not in par_columns:
            raise SystemExit(f"Missing column in partner CSV: {col}")

    all_ts = [parse_timestamp(r["タイムスタンプ"].strip()) for r in app_rows + par_rows]
    if not all_ts:
        raise SystemExit("No data rows found")

    start = term_start_from_timestamp(min(all_ts))

    apps, pars = build_submissions(app_rows, par_rows, start)

    max_term = max([s.term for s in apps + pars])
    ineligible_ids: set = set()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for term in range(max_term + 1):
        term_apps = [s for s in apps if s.term == term]
        term_pars = [s for s in pars if s.term == term]

        processed_apps, processed_pars, valid_rows = process_term(
            term, term_apps, term_pars, ineligible_ids
        )

        # Update ineligible IDs for later terms
        for app_id, _, par_id, _, _ in valid_rows:
            ineligible_ids.add(app_id)
            if par_id:
                ineligible_ids.add(par_id)

        term_dir = OUTPUT_DIR / f"term{term + 1}"
        term_dir.mkdir(parents=True, exist_ok=True)

        write_valid(term_dir, valid_rows)
        write_invalid(term_dir / "invalid_app.csv", APP_COLUMNS, processed_apps)
        write_invalid(term_dir / "invalid_par.csv", PAR_COLUMNS, processed_pars)


if __name__ == "__main__":
    main()
