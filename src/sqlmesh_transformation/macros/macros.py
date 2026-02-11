from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator
from sqlglot import exp


def _col(alias: str, name: str) -> exp.Column:
    return exp.Column(
        this=exp.Identifier(this=name),
        table=exp.Identifier(this=alias),
    )


@macro()
def as_table(evaluator: MacroEvaluator, model: exp.Expression) -> exp.Expression:
    """
    Converts things like silver.client (often parsed as a Column) or 'silver.client' (string literal)
    into a proper Table expression that renders as "silver"."client".
    """
    # Case 1: parsed like a Column: silver.client  -> Column(table="silver", this="client")
    if isinstance(model, exp.Column) and model.table:
        schema = model.table
        table = model.name
        return exp.Table(this=exp.to_identifier(table), db=exp.to_identifier(schema))

    # Case 2: string literal: 'silver.client'
    s = model.sql(dialect=evaluator.dialect).strip().strip("'").strip('"')
    parts = [p for p in s.split(".") if p]
    if len(parts) >= 2:
        schema, table = parts[-2], parts[-1]
        return exp.Table(this=exp.to_identifier(table), db=exp.to_identifier(schema))

    # Fallback: let it render as-is
    return model

@macro()
def fk_join(evaluator: MacroEvaluator, child_alias: str, parent_alias: str, mappings: list[tuple[str, str]]):
    """
    Returns an AND'ed join predicate like:
      c.child1 = p.parent1 AND c.child2 = p.parent2 ...
    mappings is a list of (child_col, parent_col) pairs.
    """
    preds = [exp.EQ(this=_col(child_alias, c), expression=_col(parent_alias, p)) for c, p in mappings]
    return exp.and_(*preds) if preds else exp.true()


@macro()
def fk_child_non_null(evaluator: MacroEvaluator, child_alias: str, mappings: list[tuple[str, str]]):
    """
    Returns an AND'ed predicate ensuring all child FK cols are NOT NULL.
    """
    preds = [_col(child_alias, c).is_(exp.null()).not_() for c, _ in mappings]
    return exp.and_(*preds) if preds else exp.true()




@macro()
def list_append(evaluator, items, item):
    """
    Append a single item to a list.

    Usage:
      @DEF(hash_exclude, @list_append(@meta_cols, '__data_snapshot_date'));
    """
    return list(items) + [item]


@macro()
def list_extend(evaluator, items, more_items):
    """
    Extend a list with another list (or tuple).

    Usage:
      @DEF(hash_exclude, @list_extend(@meta_cols, ['__data_snapshot_date', '__rn']));
    """
    return list(items) + list(more_items)