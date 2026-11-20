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
Seed loading
============

A CSV is loaded in up to three stages:

1. Rows are rendered once, with the same cell literals the single-statement
   path has always produced, and grouped by the adapter's `plan_seed_batches`
   against both a row count (`seed_batch_size`, default 10000) and a UTF-8
   byte ceiling (`seed_batch_max_bytes`, default 1 MiB).  A single row that
   cannot fit is a hard error naming the row and the column responsible;
   nothing is ever truncated.
2. A seed that fits in one batch is built with a single CTAS, exactly as
   before (Dremio publishes a CTAS atomically, so it is already
   complete-on-publish).  A larger seed is written in batches to a *candidate*
   relation (`<alias>__dbt_seed_candidate`): the first batch with CTAS, the
   rest with INSERT.  Only after every batch succeeds, and the candidate is
   verified to hold every CSV row, is it published to the target.  A failure
   part-way leaves the target untouched (the previous complete seed stays
   readable) and the node errors, so a partial load is never reported as the
   new seed.
3. Metadata refresh, twin strategy, docs and grants run on the published
   target relation, as they did before.
#}

{% materialization seed, adapter = 'dremio' %}

  {%- set identifier = model['alias'] -%}
  {%- set format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}
  {%- set num_rows = (agate_table.rows | length) -%}

  {# Render every cell once; the batched and single paths share these literals,
     and the same rendered cells let an oversized row name the column that
     caused it. #}
  {%- set rendered_rows = [] -%}
  {%- set row_values = [] -%}
  {%- for row in agate_table.rows -%}
    {%- set cells = render_seed_row_cells(row) -%}
    {%- do row_values.append(cells) -%}
    {%- do rendered_rows.append('(' ~ (cells | join(',')) ~ ')') -%}
  {%- endfor -%}

  {# The candidate is named here only to size the CTAS; it is created below. #}
  {%- set candidate_identifier = target_relation.identifier ~ "__dbt_seed_candidate" -%}
  {%- set candidate_relation = target_relation.incorporate(
      path={"identifier": candidate_identifier}, type='table') -%}
  {%- set ctas_parts = seed_ctas_parts(candidate_relation) -%}
  {%- set batches = adapter.plan_seed_batches(
      rendered_rows=rendered_rows
      ,column_names=agate_table.column_names
      ,row_limit=config.get('seed_batch_size')
      ,max_bytes=config.get('seed_batch_max_bytes')
      ,seed_name=identifier
      ,statement_prefixes=[
        ctas_parts.prefix ~ seed_select_prefix(model, agate_table)
        ,seed_insert_prefix(candidate_relation, model, agate_table)
      ]
      ,statement_suffixes=[seed_values_suffix(model, agate_table) ~ ctas_parts.suffix]
      ,row_values=row_values
  ) -%}
  {%- set batched = (batches | length) > 1 -%}

  {%- if batched -%}
    {%- set build_relation = candidate_relation -%}
    -- a stale candidate from an interrupted run must never be mistaken for progress
    {{ drop_relation_if_exists(load_cached_relation(candidate_relation)) }}
  {%- else -%}
    {%- set build_relation = target_relation -%}
    -- single-statement path keeps its historical drop-before-create behaviour
    {% if old_relation is not none %}
      {{ adapter.drop_relation(old_relation) }}
    {% endif %}
  {%- endif -%}

  {%- set build_sqls = [] -%}
  {%- if num_rows == 0 -%}
    {%- do build_sqls.append(
        create_table_as(False, build_relation, seed_empty_select(model, agate_table))) -%}
  {%- else -%}
    {%- for batch in batches -%}
      {%- set batch_rows = rendered_rows[batch.start:batch.start + batch.count] | join(',') -%}
      {%- if loop.first -%}
        {%- do build_sqls.append(
            create_table_as(False, build_relation, seed_batch_select(model, agate_table, batch_rows))) -%}
      {%- else -%}
        {%- do build_sqls.append(
            seed_batch_insert(build_relation, model, agate_table, batch_rows)) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {{ log('Loading seed ' ~ identifier ~ ' as ' ~ (build_sqls | length) ~ ' statement(s)'
      ~ ' (' ~ num_rows ~ ' rows, ' ~ ('batched' if batched else 'single') ~ ' path)'
      ~ ((' at most ' ~ batches[0].count ~ ' rows and ' ~ batches[0].max_bytes ~ ' bytes per statement') if batched else ''),
      info=true) }}

  {%- for batch_sql in build_sqls -%}
    {%- if loop.index0 < (batches | length) -%}
      {%- do check_seed_statement_size(batch_sql, batches[loop.index0], identifier) -%}
    {%- endif -%}
    {%- if loop.first -%}
      {% call statement('effective_main') -%}
        {{ batch_sql }}
      {%- endcall %}
    {%- else -%}
      {% call statement('effective_batch_' ~ loop.index) -%}
        {{ batch_sql }}
      {%- endcall %}
    {%- endif -%}
  {%- endfor -%}

  {%- if batched -%}
    {#-- Every batch statement reported success. Confirm the candidate really
         holds every CSV row before anything is published; a short load must
         fail the node, not masquerade as the new seed. #}
    {%- set candidate_count = seed_count_rows(candidate_relation) -%}
    {%- if candidate_count != num_rows -%}
      {{ drop_relation_if_exists(load_cached_relation(candidate_relation)) }}
      {{ exceptions.raise_compiler_error(
        "Seed '" ~ identifier ~ "' loaded " ~ candidate_count ~ " of " ~ num_rows ~
        " rows into its candidate table. Nothing was published; the previous seed (if any) is unchanged."
      ) }}
    {%- endif -%}

    {#-- Publish. The candidate is already the complete new seed, so the old
         target is dropped and the candidate brought in under its name. On
         Dremio `rename_relation` is create-as-select + drop, so this costs
         one copy of the new data. Doing it the incremental way (target to
         backup, candidate to target) would copy the old seed as well for no
         benefit: a seed is always a full rebuild. #}
    {% if old_relation is not none %}
      {{ drop_relation_if_exists(old_relation) }}
    {% endif %}
    {% do adapter.rename_relation(candidate_relation, target_relation) %}
  {%- endif -%}

  {% call noop_statement('main', 'CREATE ' ~ num_rows, 'CREATE', num_rows) %}
    {% for batch_sql in build_sqls %}{{ batch_sql }};
    {% endfor %}
  {% endcall %}

  {{ refresh_metadata(target_relation, format) }}

  {{ apply_twin_strategy(target_relation) }}

  {% do persist_docs(target_relation, model) %}

  {# Seeds always land in a brand-new table (single path drops first, batched
     path publishes a fresh candidate), so no old grant can have carried over:
     grant what is configured rather than diffing against stale state. #}
  {% do apply_grants(target_relation, grant_config, should_revoke=false) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
