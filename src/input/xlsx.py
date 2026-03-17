from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


_INN_RE = re.compile(r"\d+")


def _resolve_missing_xlsx(path: Path) -> Path:
    """
    If the requested xlsx file does not exist, try to fall back to a single .xlsx
    file in the same directory (common for Docker bind-mount setups).
    """
    parent = path.parent
    if not parent.exists() or not parent.is_dir():
        raise FileNotFoundError(str(path))

    candidates = sorted([p for p in parent.glob("*.xlsx") if p.is_file()])
    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise FileNotFoundError(f"{path} (no .xlsx files found in {parent})")

    names = ", ".join(p.name for p in candidates[:10])
    suffix = "" if len(candidates) <= 10 else f", ... (+{len(candidates) - 10} more)"
    raise FileNotFoundError(f"{path} (available in {parent}: {names}{suffix})")


def normalize_inn(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    digits = "".join(_INN_RE.findall(s))
    if not digits:
        return None
    return digits


def load_inns_from_xlsx(path: Path, *, column: str | None = None) -> list[str]:
    if not path.exists():
        path = _resolve_missing_xlsx(path)

    df = pd.read_excel(path, engine="openpyxl")
    if df.empty:
        return []

    columns_by_str: dict[str, object] = {str(c): c for c in df.columns}

    if column is None:
        lowered = {str(c).strip().lower(): c for c in df.columns}
        for key in ("инн", "inn", "taxid", "tax_id"):
            if key in lowered:
                column = str(lowered[key])
                break
        if column is None:
            column = str(df.columns[0])

    if column not in columns_by_str:
        raise ValueError(
            f"Column not found in xlsx: {column!r}. Available: {[str(c) for c in df.columns]!r}"
        )
    real_column = columns_by_str[column]

    inns: list[str] = []
    seen: set[str] = set()
    for raw in df[real_column].tolist():
        inn = normalize_inn(raw)
        if inn and inn not in seen:
            seen.add(inn)
            inns.append(inn)
    return inns


def load_case_numbers_from_xlsx(path: Path) -> list[str]:
    if not path.exists():
        path = _resolve_missing_xlsx(path)

    df = pd.read_excel(path, engine="openpyxl")
    if df.empty:
        return []

    real_column = df.columns[0]

    cases: list[str] = []
    seen: set[str] = set()
    for raw in df[real_column].tolist():
        if raw is None:
            continue
        value = str(raw).strip()
        if not value or value.lower() in ("nan", "none"):
            continue
        if value not in seen:
            seen.add(value)
            cases.append(value)
    return cases

