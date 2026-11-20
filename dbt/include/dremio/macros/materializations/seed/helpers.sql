/*Copyright (C) 2022 Dremio Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

{#
Rendering helpers shared by the single-statement and the batched seed paths.

Cell literals and the VALUES scaffolding are produced by exactly one set of
macros, so a batched load emits the same literals - quotes doubled, NULL as
`cast(null as varchar)`, empty strings and non-ASCII text untouched - as the
legacy `select_csv_rows` statement.  The batching decision itself (row count
and UTF-8 byte budget) is made by the adapter's `plan_seed_batches` method;
the strings handed to it are the very strings concatenated into the executed
SQL.
#}

{% macro render_seed_cell(value) -%}
{%- if value is not none -%}
{{ "'" ~ (value | string | replace("'", "''")) ~ "'" }}
{%- else -%}
{{ 'cast(null as varchar)' }}
{%- endif -%}
{%- endmacro %}

{% macro render_seed_row_cells(row) %}
  {%- set cells = [] -%}
  {%- for value in row -%}
    {%- do cells.append(render_seed_cell(value)) -%}
  {%- endfor -%}
  {{ return(cells) }}
{% endmacro %}

{% macro render_seed_row(row) -%}
{{ '(' ~ (render_seed_row_cells(row) | join(',')) ~ ')' }}
{%- endmacro %}

{% macro seed_cast_expressions(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set expressions = [] -%}
  {%- for col_name in agate_table.column_names -%}
    {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
    {%- set type = column_override.get(col_name, inferred_type) -%}
    {%- set column_name = (col_name | string) -%}
    {%- set expression -%}
      cast({{ adapter.quote_seed_column(column_name, quote_seed_column) }} as {{ type }})
        as {{ adapter.quote_seed_column(column_name, quote_seed_column) }}
    {%- endset -%}
    {%- do expressions.append(expression) -%}
  {%- endfor -%}
  {{ return(expressions) }}
{% endmacro %}

{% macro seed_select_prefix(model, agate_table) -%}
{%- set expressions = seed_cast_expressions(model, agate_table) -%}
{{ 'select ' ~ (expressions | join(', ')) ~ ' from (values ' }}
{%- endmacro %}

{% macro seed_values_suffix(model, agate_table) -%}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
{{ ' ) temp_table ( ' ~ cols_sql ~ ' )' }}
{%- endmacro %}

{% macro seed_insert_prefix(target_relation, model, agate_table) -%}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
{{ 'insert into ' ~ target_relation ~ ' ( ' ~ cols_sql ~ ' ) ' }}
{%- endmacro %}

{% macro seed_batch_select(model, agate_table, rows_sql) -%}
{{ seed_select_prefix(model, agate_table) ~ rows_sql ~ seed_values_suffix(model, agate_table) }}
{%- endmacro %}

{#
Follow-on batches insert the same cast expressions the CTAS batch selected, so
every row goes through identical type conversion instead of relying on implicit
casts from string literals into decimal/timestamp/boolean columns.
#}
{% macro seed_batch_insert(target_relation, model, agate_table, rows_sql) -%}
{{ seed_insert_prefix(target_relation, model, agate_table) ~ seed_batch_select(model, agate_table, rows_sql) }}
{%- endmacro %}

{#
A zero-row seed still needs a table with the inferred column types, so cast
explicit NULLs instead of selecting from an empty VALUES list.
#}
{% macro seed_empty_select(model, agate_table) -%}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}
  {%- set expressions = [] -%}
  {%- for col_name in agate_table.column_names -%}
    {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
    {%- set type = column_override.get(col_name, inferred_type) -%}
    {%- set column_name = (col_name | string) -%}
    {%- do expressions.append(
        'cast(null as ' ~ type ~ ') as ' ~ adapter.quote_seed_column(column_name, quote_seed_column)) -%}
  {%- endfor -%}
{{ 'select ' ~ (expressions | join(', ')) ~ ' from (values (1)) temp_table (seed_empty_marker) where 1 = 0' }}
{%- endmacro %}

{% macro seed_count_rows(relation) -%}
  {%- set result = run_query('select count(*) as row_count from ' ~ relation) -%}
  {{ return(result.rows[0][0]) }}
{%- endmacro %}

{#
The immutable parts of the CTAS statement, so the batched loader can size it
before rows are attached.

The prefix and suffix are obtained by rendering `create_table_as` around a
sentinel body and splitting on it, so the bytes counted are exactly the bytes
that will be sent (sql_header, partition/store clauses, whitespace, closing
parens) rather than a hand-copied mirror.  A contract-enforced CTAS validates
the column list of the SQL it wraps, which needs a live query and so cannot run
against a sentinel; that (uncommon) case rebuilds the wrapper from the same
macros and relies on `check_seed_statement_size`, which measures the finished
statement.
#}
{% macro seed_ctas_parts(relation) -%}
  {%- set contract_config = config.get('contract') -%}
  {%- if not contract_config.enforced -%}
    {%- set marker = '__dbt_seed_marker_7f3a__' -%}
    {%- set rendered = create_table_as(False, relation, marker) -%}
    {%- if marker in rendered -%}
      {%- set parts = rendered.split(marker) -%}
      {{ return({'prefix': parts[0], 'suffix': parts[1]}) }}
    {%- endif -%}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set header_parts = [] -%}
  {%- if sql_header is not none -%}
    {%- do header_parts.append(sql_header | string) -%}
  {%- endif -%}
  {%- do header_parts.append('create table ' ~ relation) -%}
  {%- do header_parts.append(partition_method() | string) -%}
  {%- do header_parts.append(config_cols("partition by") | string) -%}
  {%- do header_parts.append(config_cols("distribute by") | string) -%}
  {%- do header_parts.append(config_cols("localsort by") | string) -%}
  {%- do header_parts.append(store_as_clause() | string) -%}
  {%- do header_parts.append(single_writer_clause() | string) -%}
  {%- do header_parts.append(get_table_columns_and_constraints() | string) -%}
  {%- do header_parts.append('as (') -%}
  {{ return({
    'prefix': header_parts | join(' '),
    'suffix': '\n  )',
  }) }}
{%- endmacro %}

{% macro check_seed_statement_size(statement_sql, batch, seed_name) -%}
  {%- set size = adapter.sql_bytes(statement_sql) -%}
  {%- if size > batch.max_bytes -%}
    {% do exceptions.raise_compiler_error(
      "Seed '" ~ seed_name ~ "' batch " ~ batch.index ~ " would need " ~ size ~
      " bytes, above seed_batch_max_bytes=" ~ batch.max_bytes ~
      ". Lower seed_batch_size or raise seed_batch_max_bytes for this seed."
    ) %}
  {%- endif -%}
  {%- do log(
    "Seed '" ~ seed_name ~ "' batch " ~ batch.index ~ ": " ~ batch.count ~
    " rows, " ~ size ~ " of " ~ batch.max_bytes ~ " bytes", info=true) -%}
{%- endmacro %}

{% macro select_csv_rows(model, agate_table) %}
{%- set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) -%}
{%- set expressions = seed_cast_expressions(model, agate_table) -%}
  select
    {{ expressions | join(', ') }}
  from
    (values
      {% for row in agate_table.rows %}
        {{ render_seed_row(row) }}{% if not loop.last %},{% endif %}
      {% endfor %}) temp_table ( {{ cols_sql }} )
{% endmacro %}
