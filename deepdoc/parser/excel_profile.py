#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from __future__ import annotations

import logging
from io import BytesIO

from deepdoc.parser.excel_parser import RAGFlowExcelParser


def _is_header_like(text: str) -> bool:
    v = text.strip()
    if not v:
        return False
    if v.isdigit():
        return False
    alpha_like = sum(1 for c in v if c.isalpha() or ("\u4e00" <= c <= "\u9fff"))
    digit_like = sum(1 for c in v if c.isdigit())
    return alpha_like >= max(1, digit_like)


def _looks_like_kv_pair_row(values: list[str]) -> bool:
    """
    Detect rows like: k1,v1,k2,v2,... used by form-like spreadsheets.
    """
    if len(values) < 2 or len(values) % 2 != 0:
        return False
    if len(values) > 12:
        return False

    key_like = 0
    value_like = 0
    pair_count = len(values) // 2

    for i in range(0, len(values), 2):
        k = values[i].strip()
        v = values[i + 1].strip()
        if not k or not v:
            continue

        # key cells are usually short labels instead of long descriptions
        if len(k) <= 24:
            key_like += 1
        # value cells can be long and may contain dates/numbers/text
        if len(v) >= 1:
            value_like += 1

    required = max(1, pair_count - 1)
    return key_like >= required and value_like >= required


def _safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _sheet_profile(rows: list, sample_limit: int = 300) -> dict:
    sampled_rows = rows[:sample_limit]
    non_empty_counts: list[int] = []
    kv_rows = 0
    table_rows = 0
    content_rows = 0

    first_row_values = []
    if sampled_rows:
        first_row_values = [_safe_str(c.value) for c in sampled_rows[0]]
        first_row_values = [v for v in first_row_values if v]

    for row in sampled_rows:
        values = [_safe_str(c.value) for c in row]
        values = [v for v in values if v]
        if not values:
            continue
        content_rows += 1
        width = len(values)
        non_empty_counts.append(width)

        if width == 1 and ("：" in values[0] or ":" in values[0]):
            kv_rows += 1
        elif width == 2:
            kv_rows += 1
        elif _looks_like_kv_pair_row(values):
            kv_rows += 1
        elif width >= 3:
            table_rows += 1

    if content_rows == 0:
        return {
            "kind": "EMPTY",
            "content_rows": 0,
            "kv_ratio": 0.0,
            "table_ratio": 0.0,
            "header_like": False,
            "avg_width": 0.0,
        }

    kv_ratio = kv_rows / content_rows
    table_ratio = table_rows / content_rows
    avg_width = sum(non_empty_counts) / max(1, len(non_empty_counts))
    header_like = bool(first_row_values) and len(first_row_values) >= 3 and sum(1 for v in first_row_values if _is_header_like(v)) >= max(2, len(first_row_values) // 2)

    if kv_ratio >= 0.65 and table_ratio <= 0.35:
        kind = "FORM_LIKE"
    elif (header_like and table_ratio >= 0.45) or table_ratio >= 0.7:
        kind = "TABLE_LIKE"
    else:
        kind = "MIXED"

    return {
        "kind": kind,
        "content_rows": content_rows,
        "kv_ratio": round(kv_ratio, 4),
        "table_ratio": round(table_ratio, 4),
        "header_like": header_like,
        "avg_width": round(avg_width, 3),
    }


def profile_excel_structure(binary: bytes, max_sheets: int = 20, sample_limit: int = 300) -> dict:
    """
    Profile spreadsheet layout and return one of:
    FORM_LIKE / TABLE_LIKE / MIXED / EMPTY.
    """
    try:
        wb = RAGFlowExcelParser._load_excel_to_workbook(BytesIO(binary))
    except Exception as e:
        logging.warning("Failed to profile excel structure: %s", e)
        return {"kind": "MIXED", "sheet_kinds": [], "signals": {"reason": "profile_error"}}

    sheet_kinds: list[str] = []
    sheet_signals: list[dict] = []

    for sheet_name in wb.sheetnames[:max_sheets]:
        ws = wb[sheet_name]
        try:
            rows = RAGFlowExcelParser._get_rows_limited(ws)
        except Exception as e:
            logging.warning("Skip sheet '%s' during profile due to rows access error: %s", sheet_name, e)
            continue
        profile = _sheet_profile(rows, sample_limit=sample_limit)
        if profile["kind"] == "EMPTY":
            continue
        sheet_kinds.append(profile["kind"])
        sheet_signals.append({"sheet": sheet_name, **profile})

    if not sheet_kinds:
        return {"kind": "EMPTY", "sheet_kinds": [], "signals": {"sheets": []}}

    unique_kinds = set(sheet_kinds)
    if len(unique_kinds) == 1:
        overall = sheet_kinds[0]
    else:
        overall = "MIXED"

    return {
        "kind": overall,
        "sheet_kinds": sheet_kinds,
        "signals": {"sheets": sheet_signals},
    }
