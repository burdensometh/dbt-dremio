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

import pytest

from dbt.adapters.dremio.seed import plan_seed_batches, sql_bytes
from dbt_common.exceptions import CompilationError


PREFIX = "select a, b from (values "
SUFFIX = ") temp_table ( a, b )"


def _plan(rows, row_limit=10_000, max_bytes=10_000_000, **kwargs):
    return plan_seed_batches(
        rendered_rows=rows,
        column_names=["a", "b"],
        row_limit=row_limit,
        max_bytes=max_bytes,
        statement_prefixes=[PREFIX],
        statement_suffixes=[SUFFIX],
        **kwargs,
    )


class TestSqlBytes:
    def test_multibyte_text_counts_encoded_bytes(self):
        assert sql_bytes("café") == 5  # 4 code points, 5 UTF-8 bytes
        assert sql_bytes("漢字") == 6
        assert sql_bytes("plain") == 5

    def test_non_string_is_coerced(self):
        assert sql_bytes(12345) == 5


class TestPlanSeedBatches:
    def test_empty_input_has_no_batches(self):
        assert _plan([]) == []

    def test_within_limits_is_a_single_batch(self):
        rows = ["(1,'a')", "(2,'b')", "(3,'c')"]
        batches = _plan(rows)
        assert len(batches) == 1
        assert batches[0]["index"] == 1
        assert batches[0]["start"] == 0
        assert batches[0]["count"] == 3

    def test_row_limit_splits_without_losing_or_reordering_rows(self):
        rows = ["(%d,'v%d')" % (i, i) for i in range(10)]
        batches = _plan(rows, row_limit=3)
        assert [b["count"] for b in batches] == [3, 3, 3, 1]
        assert [b["start"] for b in batches] == [0, 3, 6, 9]
        assert [b["index"] for b in batches] == [1, 2, 3, 4]
        # every row lands in exactly one batch, in order
        rebuilt = []
        for batch in batches:
            rebuilt.extend(rows[batch["start"] : batch["start"] + batch["count"]])
        assert rebuilt == rows

    def test_byte_limit_splits_before_exceeding_the_ceiling(self):
        rows = ["('%s','%s')" % ("x" * 40, "y" * 40) for _ in range(5)]
        max_bytes = 300
        batches = _plan(rows, max_bytes=max_bytes)
        assert len(batches) > 1
        for batch in batches:
            assert batch["bytes"] <= max_bytes
        rebuilt = []
        for batch in batches:
            rebuilt.extend(rows[batch["start"] : batch["start"] + batch["count"]])
        assert rebuilt == rows

    def test_byte_limit_counts_multibyte_characters(self):
        ascii_rows = ["('%s',1)" % ("a" * 50) for _ in range(4)]
        cjk_rows = ["('%s',1)" % ("漢" * 50) for _ in range(4)]
        max_bytes = 400
        ascii_batches = _plan(ascii_rows, max_bytes=max_bytes)
        cjk_batches = _plan(cjk_rows, max_bytes=max_bytes)
        # CJK rows use 3 bytes per character, so more batches are needed
        assert len(cjk_batches) > len(ascii_batches)
        for batch in cjk_batches:
            assert batch["bytes"] <= max_bytes

    def test_row_limit_and_byte_limit_are_both_honoured(self):
        rows = ["('%s')" % ("x" * 30) for _ in range(20)]
        batches = _plan(rows, row_limit=4, max_bytes=200)
        for batch in batches:
            assert batch["count"] <= 4
            assert batch["bytes"] <= 200

    def test_plan_is_deterministic(self):
        rows = ["('%d')" % i for i in range(50)]
        first = _plan(rows, row_limit=7, max_bytes=200)
        second = _plan(rows, row_limit=7, max_bytes=200)
        assert first == second
        assert len(first) > 1

    def test_prefix_and_suffix_bytes_are_reserved(self):
        rows = ["('value')" for _ in range(3)]
        overhead_bytes = sql_bytes(PREFIX) + sql_bytes(SUFFIX)
        batches = plan_seed_batches(
            rendered_rows=rows,
            column_names=["a", "b"],
            row_limit=100,
            max_bytes=overhead_bytes + 64 + sql_bytes("('value'),('value')") + 1,
            statement_prefixes=[PREFIX],
            statement_suffixes=[SUFFIX],
        )
        assert len(batches) >= 2

    def test_oversized_row_is_located_and_never_truncated(self):
        rows = ["('ok',1)", "('" + "x" * 500 + "',2)", "('ok',3)"]
        with pytest.raises(CompilationError) as excinfo:
            _plan(
                rows,
                max_bytes=300,
                row_values=[("ok", 1), ("x" * 500, 2), ("ok", 3)],
            )
        message = str(excinfo.value)
        assert "row 2" in message  # 1-based, points at the offending row
        assert "'a'" in message  # names the column carrying the oversized value
        assert "seed_batch_max_bytes=300" in message
        assert "never truncated" in message

    def test_oversized_row_reports_without_source_values(self):
        rows = ["('" + "z" * 500 + "')"]
        with pytest.raises(CompilationError) as excinfo:
            _plan(rows, max_bytes=200)
        assert "row 1" in str(excinfo.value)

    def test_scaffolding_larger_than_ceiling_is_rejected(self):
        with pytest.raises(CompilationError) as excinfo:
            _plan(["('a')"], max_bytes=len(PREFIX) + len(SUFFIX))
        assert "scaffolding" in str(excinfo.value)

    def test_invalid_limits_are_rejected(self):
        for kwargs, field in [
            ({"row_limit": 0}, "seed_batch_size"),
            ({"row_limit": -1}, "seed_batch_size"),
            ({"row_limit": True}, "seed_batch_size"),
            ({"row_limit": "10"}, "seed_batch_size"),
            ({"max_bytes": 0}, "seed_batch_max_bytes"),
            ({"max_bytes": 1.5}, "seed_batch_max_bytes"),
        ]:
            with pytest.raises(CompilationError) as excinfo:
                _plan(["('a')"], **kwargs)
            assert field in str(excinfo.value)

    def test_quotes_null_and_empty_values_are_preserved_verbatim(self):
        # The planner only groups rendered literals; it must hand them back
        # byte-for-byte, including doubled quotes, NULL casts and ''.
        rows = [
            "('O''Brien')",
            "(cast(null as varchar))",
            "('')",
            "('café 漢字')",
        ]
        batches = _plan(rows, row_limit=1)
        rebuilt = []
        for batch in batches:
            rebuilt.extend(rows[batch["start"] : batch["start"] + batch["count"]])
        assert rebuilt == rows
