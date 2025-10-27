import os
from typing import List, Dict, Tuple, Any, Optional

import pandas as pd
import streamlit as st

from common import run_sql as sqlQuery

st.set_page_config(page_title="Edit Data", layout="wide")


# ---------- Helpers ----------
@st.cache_data(ttl=30)
def list_tables(catalog: str, schema: Optional[str] = None) -> pd.DataFrame:
    where_schema = (
        f"and table_schema = '{schema}'\n" if schema and len(schema.strip()) > 0 else ""
    )
    query = f"""
    select table_schema, table_name
    from `{catalog}`.information_schema.tables
    where table_type = 'MANAGED'
    {where_schema}order by table_schema, table_name
    """
    return sqlQuery(query)


@st.cache_data(ttl=30)
def get_columns(catalog: str, schema: str, table: str) -> pd.DataFrame:
    query = f"""
    select column_name, data_type, is_nullable
    from `{catalog}`.information_schema.columns
    where table_schema = '{schema}' and table_name = '{table}'
    order by ordinal_position
    """
    return sqlQuery(query)


@st.cache_data(ttl=30)
def get_primary_keys(catalog: str, schema: str, table: str) -> List[str]:
    # Try to fetch PK columns from information_schema
    query = f"""
    select kcu.column_name
    from `{catalog}`.information_schema.table_constraints tc
    join `{catalog}`.information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema = kcu.table_schema
     and tc.table_name = kcu.table_name
    where tc.table_schema = '{schema}'
      and tc.table_name = '{table}'
      and tc.constraint_type = 'PRIMARY KEY'
    order by kcu.ordinal_position
    """
    try:
        df = sqlQuery(query)
        return df["column_name"].tolist() if df is not None and len(df) else []
    except Exception:
        # Unity Catalog may not return constraints for some tables; fall back to none
        return []


@st.cache_data(ttl=30)
def get_table_data(
    catalog: str, schema: str, table: str, limit: int, columns_for_hash: List[str]
) -> pd.DataFrame:
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    # Build named_struct for stable row hashing on the server side
    def _named_struct(cols: List[str], alias: str = "t") -> str:
        parts = []
        for c in cols:
            parts.append(f"'{c}', {alias}.{_sql_ident(c)}")
        return f"named_struct({', '.join(parts)})"

    hash_expr = f"sha2(to_json({_named_struct(columns_for_hash)}), 256) as _row_hash"
    query = f"select {hash_expr}, t.* from {fqn} t limit {int(limit)}"
    return sqlQuery(query)


def _sql_ident(name: str) -> str:
    return f"`{name}`"


def _sql_fqn(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def _sql_literal(value: Any) -> str:
    if pd.isna(value):
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if pd.isna(value) or pd.isnull(value):
            return "NULL"
        return "%r" % value
    # Dates, timestamps, decimals and everything else as strings
    s = str(value)
    s = s.replace("'", "''")
    return f"'{s}'"


def _row_hash_expr(columns: List[str], table_alias: Optional[str] = None) -> str:
    """Return Spark SQL expression that computes the row hash for given columns."""
    alias = table_alias or ""
    alias_prefix = f"{alias}." if alias else ""
    parts = []
    for c in columns:
        parts.append(f"'{c}', {alias_prefix}{_sql_ident(c)}")
    named = f"named_struct({', '.join(parts)})"
    return f"sha2(to_json({named}), 256)"


def compute_changes(
    original: pd.DataFrame, edited: pd.DataFrame, key_cols: List[str]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (inserts, updates, deletes) where each entry is a row dict of column->value.
    Updates contain both key values and changed non-key columns.
    """
    if not key_cols:
        raise ValueError("Key columns must be specified to compute changes.")

    # Ensure keys exist
    for k in key_cols:
        if k not in original.columns or k not in edited.columns:
            raise ValueError(f"Key column '{k}' not present in data")

    # Index by keys
    orig_idxed = original.set_index(key_cols, drop=False)
    edit_idxed = edited.set_index(key_cols, drop=False)

    # Uniqueness check
    if not orig_idxed.index.is_unique:
        raise ValueError(
            "Original data has duplicate key values. Please choose different key columns."
        )
    if not edit_idxed.index.is_unique:
        raise ValueError(
            "Edited data has duplicate key values. Please resolve duplicates before saving."
        )

    orig_keys = set(orig_idxed.index.tolist())
    edit_keys = set(edit_idxed.index.tolist())

    to_delete_keys = sorted(orig_keys - edit_keys)
    to_insert_keys = sorted(edit_keys - orig_keys)
    maybe_update_keys = sorted(orig_keys & edit_keys)

    inserts: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    deletes: List[Dict[str, Any]] = []

    # Inserts
    for key in to_insert_keys:
        row = edit_idxed.loc[key]
        # When set_index with multiple keys and single match, row is Series
        if isinstance(row, pd.Series):
            inserts.append(row.to_dict())
        else:
            # Shouldn't happen due to uniqueness check
            for _, r in row.iterrows():
                inserts.append(r.to_dict())

    # Deletes
    for key in to_delete_keys:
        row = orig_idxed.loc[key]
        if isinstance(row, pd.Series):
            deletes.append({k: row[k] for k in key_cols})
        else:
            for _, r in row.iterrows():
                deletes.append({k: r[k] for k in key_cols})

    # Updates
    non_key_cols = [c for c in original.columns if c not in key_cols]
    for key in maybe_update_keys:
        o_row = orig_idxed.loc[key]
        e_row = edit_idxed.loc[key]
        if isinstance(o_row, pd.DataFrame) or isinstance(e_row, pd.DataFrame):
            # Defensive; uniqueness should prevent this
            o_row = o_row.iloc[0] if isinstance(o_row, pd.DataFrame) else o_row
            e_row = e_row.iloc[0] if isinstance(e_row, pd.DataFrame) else e_row
        changes = {}
        for c in non_key_cols:
            o_val = o_row[c]
            e_val = e_row[c]
            # Treat NaN == None
            if pd.isna(o_val) and pd.isna(e_val):
                continue
            if (pd.isna(o_val) and not pd.isna(e_val)) or (
                not pd.isna(o_val) and pd.isna(e_val)
            ):
                changes[c] = e_val
            elif o_val != e_val:
                changes[c] = e_val
        if changes:
            # Include keys
            for k in key_cols:
                changes[k] = e_row[k]
            updates.append(changes)

    return inserts, updates, deletes


def compute_changes_by_index(
    original: pd.DataFrame, edited: pd.DataFrame
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Diff two DataFrames using their index as the row identifier (original row hash).
    Returns (inserts, updates, deletes).
    Each update dict contains a special key '__row_hash' with the original index value.
    Each delete dict contains only '__row_hash'.
    Inserts are full row dicts of the edited values.
    """
    if original.index.name is None:
        original.index.name = "_row_hash"
    if edited.index.name is None:
        edited.index.name = "_row_hash"

    # Ensure no duplicate indices
    if not original.index.is_unique:
        raise ValueError(
            "Original data has duplicate synthetic keys; cannot compute diff."
        )
    if not edited.index.is_unique:
        # Streamlit may produce duplicate indices for new rows; keep first occurrence
        edited = edited[~edited.index.duplicated(keep="first")]

    orig_keys = set(original.index.tolist())
    edit_keys = set(edited.index.tolist())

    to_delete_keys = sorted(orig_keys - edit_keys)
    to_insert_keys = sorted(edit_keys - orig_keys)
    maybe_update_keys = sorted(orig_keys & edit_keys)

    inserts: List[Dict[str, Any]] = []
    updates: List[Dict[str, Any]] = []
    deletes: List[Dict[str, Any]] = []

    # Inserts: take entire row values
    for key in to_insert_keys:
        row = edited.loc[key]
        if isinstance(row, pd.Series):
            inserts.append(row.to_dict())
        else:
            for _, r in row.iterrows():
                inserts.append(r.to_dict())

    # Deletes: only identifier
    for key in to_delete_keys:
        deletes.append({"__row_hash": key})

    # Updates: compare non-index columns
    non_index_cols = [c for c in original.columns]
    for key in maybe_update_keys:
        o_row = original.loc[key]
        e_row = edited.loc[key]
        if isinstance(o_row, pd.DataFrame) or isinstance(e_row, pd.DataFrame):
            o_row = o_row.iloc[0] if isinstance(o_row, pd.DataFrame) else o_row
            e_row = e_row.iloc[0] if isinstance(e_row, pd.DataFrame) else e_row
        changes: Dict[str, Any] = {}
        for c in non_index_cols:
            o_val = o_row[c]
            e_val = e_row[c]
            if pd.isna(o_val) and pd.isna(e_val):
                continue
            if (pd.isna(o_val) and not pd.isna(e_val)) or (
                not pd.isna(o_val) and pd.isna(e_val)
            ):
                changes[c] = e_val
            elif o_val != e_val:
                changes[c] = e_val
        if changes:
            changes["__row_hash"] = key
            updates.append(changes)

    return inserts, updates, deletes


def apply_changes(
    catalog: str,
    schema: str,
    table: str,
    columns: List[str],
    key_cols: List[str],
    inserts: List[Dict[str, Any]],
    updates: List[Dict[str, Any]],
    deletes: List[Dict[str, Any]],
) -> None:
    fqn = _sql_fqn(catalog, schema, table)

    # Inserts
    if inserts:
        cols_sql = ", ".join(_sql_ident(c) for c in columns)
        for row in inserts:
            vals_sql = ", ".join(_sql_literal(row.get(c)) for c in columns)
            sqlQuery(f"INSERT INTO {fqn} ({cols_sql}) VALUES ({vals_sql})")

    # Updates
    if updates:
        non_key_cols = [c for c in columns if c not in key_cols]
        for row in updates:
            set_exprs = []
            for c in non_key_cols:
                if c in key_cols:
                    continue
                if c in row:
                    set_exprs.append(f"{_sql_ident(c)} = {_sql_literal(row[c])}")
            if not set_exprs:
                continue
            where_exprs = []
            for k in key_cols:
                v = row[k]
                if pd.isna(v):
                    where_exprs.append(f"{_sql_ident(k)} IS NULL")
                else:
                    where_exprs.append(f"{_sql_ident(k)} = {_sql_literal(v)}")
            sqlQuery(
                f"UPDATE {fqn} SET {', '.join(set_exprs)} WHERE {' AND '.join(where_exprs)}"
            )

    # Deletes
    if deletes:
        for row in deletes:
            where_exprs = []
            for k in key_cols:
                v = row[k]
                if pd.isna(v):
                    where_exprs.append(f"{_sql_ident(k)} IS NULL")
                else:
                    where_exprs.append(f"{_sql_ident(k)} = {_sql_literal(v)}")
            sqlQuery(f"DELETE FROM {fqn} WHERE {' AND '.join(where_exprs)}")


# ---------- UI ----------

st.title("Edit data in your catalog")

catalog = os.getenv("CATALOG_NAME")
fixed_schema = os.getenv("SCHEMA_NAME")
if not catalog:
    st.error("CATALOG_NAME environment variable is not set. Add it to your .env file.")
    st.stop()

# Table selection
try:
    tables_df = list_tables(catalog, fixed_schema)
except Exception as e:
    st.error(f"Failed to list tables for catalog '{catalog}': {e}")
    st.stop()

if tables_df is None or tables_df.empty:
    if fixed_schema:
        st.info(
            f"No base tables found in catalog '{catalog}' and schema '{fixed_schema}'."
        )
    else:
        st.info(f"No base tables found in catalog '{catalog}'.")
    st.stop()

# Build options like schema.table
tables_df["qualified"] = tables_df["table_schema"] + "." + tables_df["table_name"]
selected_qualified_opt = st.selectbox(
    "Choose a table to edit"
    if not fixed_schema
    else f"Choose a table to edit (schema: {fixed_schema})",
    options=tables_df["qualified"].tolist(),
    index=0,
)

if not isinstance(selected_qualified_opt, str) or not selected_qualified_opt:
    st.stop()
selected_qualified = str(selected_qualified_opt)
if "." not in selected_qualified:
    st.stop()
schema, table = selected_qualified.split(".", 1)

limit = st.number_input("Row limit", min_value=10, max_value=10000, value=500, step=10)

with st.spinner("Loading table metadata and data..."):
    cols_df = get_columns(catalog, schema, table)
    all_columns: List[str] = (
        cols_df["column_name"].tolist() if cols_df is not None else []
    )
    detected_pk = get_primary_keys(catalog, schema, table)
    data = get_table_data(catalog, schema, table, limit, all_columns)

if data is None:
    st.warning("No data returned.")
    st.stop()

"""Use server-computed _row_hash as index for diffing and hide from view later."""
if "_row_hash" in data.columns:
    data = data.set_index("_row_hash", drop=True)

st.caption(
    f"Editing: `{catalog}`.`{schema}`.`{table}` — using content-hash for row identity (no PK required)"
)

# Keep a copy of original for diffing
original_df = data.copy()

st.subheader("Data editor")
edited_df = st.data_editor(
    data,
    num_rows="dynamic",  # allow adds
    height=500,
    hide_index=True,
)

colA, colB = st.columns([1, 3])
with colA:
    refresh = st.button("Reload data", type="secondary")
with colB:
    save = st.button("Save changes", type="primary")

if refresh:
    list_tables.clear()
    get_columns.clear()
    get_primary_keys.clear()
    get_table_data.clear()
    st.rerun()

if save:
    try:
        inserts, updates, deletes = compute_changes_by_index(original_df, edited_df)

        if not any([inserts, updates, deletes]):
            st.info("No changes to save.")
        else:
            with st.spinner("Applying changes..."):
                # Apply by content-hash matching
                def apply_changes_by_rowhash(
                    catalog: str,
                    schema: str,
                    table: str,
                    columns: List[str],
                    inserts: List[Dict[str, Any]],
                    updates: List[Dict[str, Any]],
                    deletes: List[Dict[str, Any]],
                ) -> None:
                    fqn = _sql_fqn(catalog, schema, table)
                    # Inserts
                    if inserts:
                        cols_sql = ", ".join(_sql_ident(c) for c in columns)
                        for row in inserts:
                            vals_sql = ", ".join(
                                _sql_literal(row.get(c)) for c in columns
                            )
                            sqlQuery(
                                f"INSERT INTO {fqn} ({cols_sql}) VALUES ({vals_sql})"
                            )

                    # Updates
                    if updates:
                        settable_cols = columns
                        rowhash_sql = _row_hash_expr(columns, table_alias="t")
                        for row in updates:
                            set_exprs = []
                            for c in settable_cols:
                                if c in row:
                                    set_exprs.append(
                                        f"{_sql_ident(c)} = {_sql_literal(row[c])}"
                                    )
                            if not set_exprs:
                                continue
                            rh = row["__row_hash"]
                            sqlQuery(
                                f"UPDATE {fqn} t SET {', '.join(set_exprs)} WHERE {rowhash_sql} = '{rh}'"
                            )

                    # Deletes
                    if deletes:
                        rowhash_sql = _row_hash_expr(columns, table_alias="t")
                        for row in deletes:
                            rh = row["__row_hash"]
                            sqlQuery(
                                f"DELETE FROM {fqn} t WHERE {rowhash_sql} = '{rh}'"
                            )

                apply_changes_by_rowhash(
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    columns=all_columns,
                    inserts=inserts,
                    updates=updates,
                    deletes=deletes,
                )
            # Clear caches and reload
            get_table_data.clear()
            st.success(
                f"Saved: +{len(inserts)} inserts, {len(updates)} updates, -{len(deletes)} deletes"
            )
            st.rerun()
    except Exception as e:
        st.error(f"Failed to save changes: {e}")
