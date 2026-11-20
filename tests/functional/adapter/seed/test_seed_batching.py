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

"""Seed loading in bounded batches.

These tests force the batched path with a tiny `seed_batch_size` (and, where
useful, a tiny `seed_batch_max_bytes`), so they exercise the same code a
multi-megabyte CSV would without needing one.
"""

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
from tests.fixtures.profiles import unique_schema, dbt_profile_data
from tests.utils.util import relation_from_name


def _seed_relation(project, name):
    return relation_from_name(project.adapter, name)


def _seed_row_count(project, name):
    relation = _seed_relation(project, name)
    return project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")[0]


def _seed_rows(project, name):
    relation = _seed_relation(project, name)
    return project.run_sql(f"select id, label from {relation} order by id", fetch="all")


def _relation_queryable(project, name):
    relation = _seed_relation(project, name)
    try:
        project.run_sql(f"select count(*) from {relation}", fetch="one")
        return True
    except Exception:
        return False


_QUOTED_AND_UNICODE_CSV = '\n'.join([
    'id,label',
    '1,"O\'Brien, Jr."',
    '2,café 漢字',
    '3,',
    '4,"line with ""double quotes"""',
    '',
])


class TestSeedBatchedLoad:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"batched.csv": _QUOTED_AND_UNICODE_CSV}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+seed_batch_size": 1}}

    def test_batched_seed_keeps_every_row_and_value(self, project):
        run_dbt(["seed"])
        assert _seed_row_count(project, "batched") == 4

        rows = _seed_rows(project, "batched")
        assert [row[0] for row in rows] == [1, 2, 3, 4]
        assert rows[0][1] == "O'Brien, Jr."
        assert rows[1][1] == "café 漢字"
        # an empty CSV field is NULL, not ''
        assert rows[2][1] is None
        assert rows[3][1] == 'line with "double quotes"'

    def test_repeated_seed_runs_converge(self, project):
        run_dbt(["seed"])
        run_dbt(["seed"])
        assert _seed_row_count(project, "batched") == 4

    def test_batched_seed_reports_one_result_per_seed(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].status == "success"


class TestSeedByteBudget:
    @pytest.fixture(scope="class")
    def seeds(self):
        # each row carries a wide text value so a small byte ceiling splits it
        rows = "\n".join("%d,%s" % (i, "x" * 120) for i in range(1, 6))
        return {"wide.csv": "id,label\n" + rows + "\n"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+seed_batch_max_bytes": 1200, "+seed_batch_size": 100}}

    def test_byte_budget_splits_without_losing_rows(self, project):
        run_dbt(["seed"])
        assert _seed_row_count(project, "wide") == 5
        rows = _seed_rows(project, "wide")
        assert [row[0] for row in rows] == [1, 2, 3, 4, 5]
        assert all(len(row[1]) == 120 for row in rows)


class TestSeedOversizedRow:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"too_wide.csv": "id,label\n1,ok\n2," + "y" * 4000 + "\n3,ok\n"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"seeds": {"+seed_batch_max_bytes": 1000, "+seed_batch_size": 100}}

    def test_oversized_row_fails_and_names_the_row(self, project):
        (results, log_output) = run_dbt_and_capture(["seed"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
        # the error pinpoints the row instead of silently truncating it
        assert "row 2" in log_output
        assert "seed_batch_max_bytes" in log_output

    def test_oversized_row_is_never_published(self, project):
        run_dbt(["seed"], expect_pass=False)
        assert not _relation_queryable(project, "too_wide")


class TestSeedFailureKeepsPreviousVersion:
    """A batch that fails mid-load must not replace the existing seed."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"replaceable.csv": "id,label\n1,first\n2,second\n3,third\n"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+seed_batch_size": 1,
                # force a numeric column so a non-numeric value in a later
                # batch makes that batch's INSERT fail
                "+column_types": {"id": "bigint"},
            }
        }

    def test_partial_failure_does_not_publish_half_a_seed(self, project):
        run_dbt(["seed"])
        assert _seed_row_count(project, "replaceable") == 3

        broken = "id,label\n1,first\n2,second\nnot_a_number,third\n"
        write_file(broken, project.project_root, "seeds", "replaceable.csv")

        (results, _) = run_dbt_and_capture(
            ["seed", "--select", "replaceable"], expect_pass=False
        )
        assert results[0].status == "error"

        # the previous, complete seed is still what downstream models read
        assert _seed_row_count(project, "replaceable") == 3
        assert [row[1] for row in _seed_rows(project, "replaceable")] == [
            "first",
            "second",
            "third",
        ]
