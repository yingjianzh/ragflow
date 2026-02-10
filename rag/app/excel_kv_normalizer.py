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


def _safe(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _row_to_kv_lines(values: list[str]) -> list[str]:
    lines: list[str] = []
    if not values:
        return lines

    if len(values) == 1:
        if "：" in values[0] or ":" in values[0]:
            lines.append(values[0])
        return lines

    # Common spreadsheet forms: [k1, v1, k2, v2, ...]
    if len(values) % 2 == 0 and len(values) <= 8:
        for i in range(0, len(values), 2):
            k = values[i].strip()
            v = values[i + 1].strip()
            if k and v:
                lines.append(f"{k}：{v}")
        if lines:
            return lines

    k = values[0].strip()
    v = "；".join([x.strip() for x in values[1:] if x.strip()])
    if k and v:
        lines.append(f"{k}：{v}")
    return lines


def normalize_excel_form_rows(binary: bytes, max_sheets: int = 20) -> list[tuple[str, str]]:
    """
    Convert form-like spreadsheet rows into KV-friendly textual sections.
    """
    try:
        wb = RAGFlowExcelParser._load_excel_to_workbook(BytesIO(binary))
    except Exception as e:
        logging.warning("Failed to normalize excel form rows: %s", e)
        return []

    sections: list[tuple[str, str]] = []
    for sheet_name in wb.sheetnames[:max_sheets]:
        ws = wb[sheet_name]
        try:
            rows = RAGFlowExcelParser._get_rows_limited(ws)
        except Exception as e:
            logging.warning("Skip sheet '%s' during form normalization due to rows access error: %s", sheet_name, e)
            continue

        if not rows:
            continue

        for row in rows:
            values = [_safe(cell.value) for cell in row]
            values = [v for v in values if v]
            if not values:
                continue
            kv_lines = _row_to_kv_lines(values)
            for line in kv_lines:
                if sheet_name.lower().find("sheet") < 0:
                    sections.append((f"{line} ——{sheet_name}", ""))
                else:
                    sections.append((line, ""))

    return sections
