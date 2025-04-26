show pipes;

-- lets check if the pipes are running or not
select system$pipe_status('location_pipe');
select system$pipe_status('restaurant_pipe');
select system$pipe_status('customer_pipe');
select system$pipe_status('customeraddress_pipe');
select system$pipe_status('menu_pipe');
select system$pipe_status('deliveryagent_pipe');
select system$pipe_status('delivery_pipe');
select system$pipe_status('orders_pipe');
select system$pipe_status('orderitem_pipe');

ALTER PIPE location_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE restaurant_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE customer_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE customeraddress_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE menu_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE deliveryagent_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE delivery_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE orders_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE orderitem_pipe SET PIPE_EXECUTION_PAUSED = TRUE;


select * from mealsmart_db.stage_sch.location_stm;
select * from mealsmart_db.CLEAN_SCH.RESTAURANT_LOCATION_STM;
select * from mealsmart_db.consumption_sch.restaurant_location_dim;

select * from mealsmart_db.stage_sch.restaurant_stm;
select * from mealsmart_db.clean_sch.restaurant_stm;
select * from mealsmart_db.consumption_sch.restaurant_dim;

select * from mealsmart_db.stage_sch.customer_stm;
select * from mealsmart_db.clean_sch.customer_stm;
select * from mealsmart_db.consumption_sch.customer_dim;

select * from mealsmart_db.stage_sch.customeraddress_stm;
select * from mealsmart_db.clean_sch.customer_address_stm;
select * from mealsmart_db.consumption_sch.customer_address_dim;

select * from mealsmart_db.stage_sch.menu;
select * from mealsmart_db.stage_sch.menu_stm;
select * from mealsmart_db.clean_sch.menu_stm;
select * from mealsmart_db.consumption_sch.menu_dim;

-- delete from mealsmart_db.stage_sch.menu where _stg_file_name ilike '%delta-load%';

select * from mealsmart_db.stage_sch.deliveryagent_stm;
select * from mealsmart_db.clean_sch.delivery_agent_stm;
select * from mealsmart_db.consumption_sch.delivery_agent_dim;

select * from mealsmart_db.stage_sch.delivery_stm;
select * from mealsmart_db.clean_sch.delivery_stm;
select * from mealsmart_db.clean_sch.delivery;

select * from mealsmart_db.stage_sch.orders_stm;
select * from mealsmart_db.clean_sch.orders_stm;
select * from mealsmart_db.clean_sch.orders;

select * from mealsmart_db.stage_sch.orderitem_stm;
select * from mealsmart_db.clean_sch.order_item_stm;
select * from mealsmart_db.clean_sch.order_item;





show tasks;

-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;
-- show grants to role sysadmin;


-- alter task mealsmart_db.stage_sch.stg_restaurant_location_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_restaurant_location_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_restaurant_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_restaurant_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_customer_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_customer_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_customeraddress_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_customer_address_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_menu_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_menu_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_delivery_agent_tsk resume;
-- alter task mealsmart_db.clean_sch.cln_delivery_agent_tsk resume;

-- alter task mealsmart_db.stage_sch.stg_delivery_tsk resume;
-- alter task mealsmart_db.stage_sch.stg_orders_tsk resume;
-- alter task mealsmart_db.stage_sch.stg_order_item_tsk resume;
-- alter task mealsmart_db.clean_sch.stg_order_item_fact_tsk resume;


-- alter task mealsmart_db.stage_sch.stg_restaurant_location_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_restaurant_location_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_restaurant_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_restaurant_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_customer_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_customer_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_customeraddress_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_customer_address_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_menu_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_menu_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_delivery_agent_tsk suspend;
-- alter task mealsmart_db.clean_sch.cln_delivery_agent_tsk suspend;

-- alter task mealsmart_db.stage_sch.stg_delivery_tsk suspend;
-- alter task mealsmart_db.stage_sch.stg_orders_tsk suspend;
-- alter task mealsmart_db.stage_sch.stg_order_item_tsk suspend;
-- alter task mealsmart_db.clean_sch.stg_order_item_fact_tsk suspend;


select *  from table(information_schema.task_history()) 
where name in ('CLN_RESTAURANT_LOCATION_TSK')
order by scheduled_time;



