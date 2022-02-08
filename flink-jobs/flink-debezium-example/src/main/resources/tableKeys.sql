select      t.table_name,
            group_concat(kcu.column_name order by kcu.ordinal_position separator ', ') as columns
from        information_schema.tables t
                left join   information_schema.table_constraints tco
                            on          t.table_schema = tco.table_schema
                                and         t.table_name = tco.table_name
                                and         tco.constraint_type = 'PRIMARY KEY'
                left join   information_schema.key_column_usage kcu
                            on          tco.constraint_schema = kcu.constraint_schema
                                and         tco.constraint_name = kcu.constraint_name
                                and         tco.table_name = kcu.table_name
where       t.table_schema = 'openmrs'
group by    t.table_name
order by    t.table_name
;
