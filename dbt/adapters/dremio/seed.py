# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Planning helpers for loading seeds in bounded batches.

The dremio seed materialization used to load every CSV row as a literal inside
a single ``VALUES`` list.  Once a CSV grows, that one statement exceeds the
size Dremio accepts for a job request.  The helpers here split the *already
rendered* row literals into batches that respect both a row count and an exact
UTF-8 byte budget, so the materialization can stage each batch into a
candidate table before publishing it.

Rows are never rewritten, truncated, or re-quoted here: the literals come from
dbt's agate table through the same `render_seed_row` macro the single-statement
path uses, and rows are only ever grouped - never sliced - so quoted text,
multi-byte characters, NULLs and empty strings reach Dremio unchanged.
"""

import typing as t
from typing import Any, Dict, List, Optional, Sequence

from dbt_common.exceptions import CompilationError

# Defaults, mirrored by the config reads in
# macros/materializations/seed/seed.sql.  Both are overridable per seed through
# the ``seed_batch_size`` (rows) and ``seed_batch_max_bytes`` (SQL bytes)
# configs.
DEFAULT_SEED_BATCH_SIZE = 10000
DEFAULT_SEED_MAX_BYTES = 1024 * 1024

# Slack folded into each planned statement size.  The seed materialization
# sizes statements from the exact CTAS/INSERT text, so this only covers the
# contract-enforced CTAS, whose wrapper cannot be rendered without a live query.
# `check_seed_statement_size` measures the finished statement regardless.
STATEMENT_BYTE_MARGIN = 64


def sql_bytes(text: str) -> int:
    """Number of UTF-8 bytes a statement fragment occupies.

    ``len()`` on a Python string counts code points, which under-counts text
    such as accented or CJK characters by 2-3x.  Request limits are expressed
    in bytes, so batching has to be decided in bytes too.
    """

    return len(str(text).encode("utf-8"))


def _validate_limit(config_name: str, value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise CompilationError(
            f"Invalid {config_name} value {value!r}: it must be a positive integer."
        )
    return value


def _largest_column(row: Sequence[Any], column_names: Sequence[str]) -> t.Tuple[str, int]:
    best_index = -1
    best_bytes = 0
    for index, value in enumerate(row):
        size = sql_bytes(value)
        if size > best_bytes:
            best_index = index
            best_bytes = size
    if 0 <= best_index < len(column_names):
        return str(column_names[best_index]), best_bytes
    return "<unknown>", best_bytes


def plan_seed_batches(
    rendered_rows: Sequence[str],
    column_names: Sequence[str],
    row_limit: int,
    max_bytes: int,
    seed_name: str = "seed",
    statement_prefixes: Optional[Sequence[str]] = None,
    statement_suffixes: Optional[Sequence[str]] = None,
    row_values: Optional[Sequence[Sequence[Any]]] = None,
) -> List[Dict[str, Any]]:
    """Split rendered seed rows into byte- and row-bounded batches.

    :param rendered_rows: one rendered SQL row literal (``(v1,v2,...)``) per CSV
        data row.
    :param column_names: CSV column names, used to pinpoint oversized rows.
    :param row_limit: maximum number of rows in a single statement.
    :param max_bytes: maximum size of a statement, in UTF-8 bytes.
    :param seed_name: seed identifier used in error messages.
    :param statement_prefixes: everything each batch statement puts *before*
        the rows (CTAS/INSERT prefixes and the SELECT casts).
    :param statement_suffixes: everything each batch statement puts *after*
        the rows (the ``temp_table`` column list).
    :param row_values: the cells behind ``rendered_rows`` (same order, either
        raw CSV values or their rendered literals), so an oversized row can
        name the column responsible for it.

    Returns one dict per batch with ``index`` (1-based), ``start`` (0-based row
    offset), ``count`` and ``bytes`` (the planned statement size).
    """

    row_limit = _validate_limit("seed_batch_size", row_limit, DEFAULT_SEED_BATCH_SIZE)
    max_bytes = _validate_limit("seed_batch_max_bytes", max_bytes, DEFAULT_SEED_MAX_BYTES)

    overhead = max((sql_bytes(p) for p in (statement_prefixes or ())), default=0) + max(
        (sql_bytes(s) for s in (statement_suffixes or ())), default=0
    ) + STATEMENT_BYTE_MARGIN

    if rendered_rows and overhead >= max_bytes:
        raise CompilationError(
            f"Seed '{seed_name}' cannot be loaded: the statement scaffolding alone "
            f"needs {overhead} bytes, which already exceeds "
            f"seed_batch_max_bytes={max_bytes}."
        )

    batches: List[Dict[str, Any]] = []
    start = 0
    used = 0
    count = 0

    def _flush() -> None:
        batches.append(
            {
                "index": len(batches) + 1,
                "start": start,
                "count": count,
                "bytes": overhead + used,
                "max_bytes": max_bytes,
            }
        )

    for index, row in enumerate(rendered_rows):
        row_bytes = sql_bytes(row)
        separator = 1 if count else 0

        if count and (
            count >= row_limit or overhead + used + separator + row_bytes > max_bytes
        ):
            _flush()
            start, used, count, separator = index, 0, 0, 0

        if overhead + row_bytes > max_bytes:
            source_row = (
                row_values[index]
                if row_values is not None and index < len(row_values)
                else ()
            )
            column_name, column_bytes = _largest_column(source_row, column_names)
            raise CompilationError(
                f"Seed '{seed_name}' row {index + 1} cannot be loaded within "
                f"seed_batch_max_bytes={max_bytes}: this single row needs "
                f"{row_bytes} bytes (largest column: {column_name!r}, "
                f"{column_bytes} bytes). Rows are never truncated; shorten that "
                f"value or raise the seed_batch_max_bytes config for this seed."
            )

        used += separator + row_bytes
        count += 1

    if count:
        _flush()

    return batches
