-- This is a conceptual example; generally, you'd use macros for reusable SQL logic, not for DDL commands

--{{ 
--  unload_to_stage('my_named_stage') 
--}}
select * from
{{ update_avg_order_amount() }};