AUDIT (
  name assert_foreign_key_same_day
);

SELECT
  c.*
FROM @this_model AS c
WHERE
  @fk_child_non_null('c', mappings := @mappings)
  AND NOT EXISTS (
    SELECT 1
    FROM @as_table(@{parent_model}) AS p
    WHERE
      @fk_join('c', 'p', mappings := @mappings)
      AND c.@{child_time_column} = p.@{parent_time_column}
  );
