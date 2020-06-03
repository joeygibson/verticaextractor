select column_name,         -- 0
       data_type,           -- 1
       data_type_length,    -- 2
       numeric_precision,   -- 3
       numeric_scale,       -- 4
       datetime_precision,  -- 5
       interval_precision   -- 6
from v_catalog.columns
where table_name = 'XX_TABLE_NAME_XX';