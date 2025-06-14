CREATE OR REPLACE TASK mealsmart_db.clean_sch.stg_order_item_fact_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.order_item_stm') 
        OR SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.orders_stm') 
        OR SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.delivery_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.consumption_sch.order_item_fact AS target
USING (
    SELECT 
        oi.Order_Item_ID AS order_item_id,
        oi.Order_ID_fk AS order_id,
        c.CUSTOMER_HK AS customer_dim_key,
        ca.CUSTOMER_ADDRESS_HK AS customer_address_dim_key,
        r.RESTAURANT_HK AS restaurant_dim_key, 
        rl.restaurant_location_hk as restaurant_location_dim_key,
        m.Menu_Dim_HK AS menu_dim_key,
        da.DELIVERY_AGENT_HK AS delivery_agent_dim_key,
        dd.DATE_DIM_HK AS order_date_dim_key,
        oi.Quantity::number(2) AS quantity,
        oi.Price AS price,
        oi.Subtotal AS subtotal,
        o.PAYMENT_METHOD,
        d.delivery_status AS delivery_status,
        d.estimated_time AS estimated_time,
    FROM 
        mealsmart_db.clean_sch.order_item_stm oi
    JOIN 
        mealsmart_db.clean_sch.orders_stm o ON oi.Order_ID_fk = o.Order_ID
    JOIN 
        mealsmart_db.clean_sch.delivery_stm d ON o.Order_ID = d.Order_ID_fk
    JOIN 
        mealsmart_db.consumption_sch.CUSTOMER_DIM c on o.Customer_ID_fk = c.customer_id
    JOIN 
        mealsmart_db.consumption_sch.CUSTOMER_ADDRESS_DIM ca on c.Customer_ID = ca.CUSTOMER_ID_fk
    JOIN 
        mealsmart_db.consumption_sch.restaurant_dim r on o.Restaurant_ID_fk = r.restaurant_id
    JOIN 
        mealsmart_db.consumption_sch.menu_dim m ON oi.MENU_ID_fk = m.menu_id
    JOIN 
        mealsmart_db.consumption_sch.delivery_agent_dim da ON d.Delivery_Agent_ID_fk = da.delivery_agent_id
    JOIN 
        mealsmart_db.consumption_sch.restaurant_location_dim rl on r.LOCATION_ID_FK = rl.location_id
    JOIN 
        mealsmart_db.CONSUMPTION_SCH.DATE_DIM dd on dd.calendar_date = date(o.order_date)
) AS source_stm
ON 
    target.order_item_id = source_stm.order_item_id and 
    target.order_id = source_stm.order_id
WHEN MATCHED THEN
    -- Update existing fact record
    UPDATE SET
        target.customer_dim_key = source_stm.customer_dim_key,
        target.customer_address_dim_key = source_stm.customer_address_dim_key,
        target.restaurant_dim_key = source_stm.restaurant_dim_key,
        target.restaurant_location_dim_key = source_stm.restaurant_location_dim_key,
        target.menu_dim_key = source_stm.menu_dim_key,
        target.delivery_agent_dim_key = source_stm.delivery_agent_dim_key,
        target.order_date_dim_key = source_stm.order_date_dim_key,
        target.quantity = source_stm.quantity,
        target.price = source_stm.price,
        target.subtotal = source_stm.subtotal,
        target.delivery_status = source_stm.delivery_status,
        target.estimated_time = source_stm.estimated_time
WHEN NOT MATCHED THEN
    -- Insert new fact record
    INSERT (
        order_item_id,
        order_id,
        customer_dim_key,
        customer_address_dim_key,
        restaurant_dim_key,
        restaurant_location_dim_key,
        menu_dim_key,
        delivery_agent_dim_key,
        order_date_dim_key,
        quantity,
        price,
        subtotal,
        delivery_status,
        estimated_time
    )
    VALUES (
        source_stm.order_item_id,
        source_stm.order_id,
        source_stm.customer_dim_key,
        source_stm.customer_address_dim_key,
        source_stm.restaurant_dim_key,
        source_stm.restaurant_location_dim_key,
        source_stm.menu_dim_key,
        source_stm.delivery_agent_dim_key,
        source_stm.order_date_dim_key,
        source_stm.quantity,
        source_stm.price,
        source_stm.subtotal,
        source_stm.delivery_status,
        source_stm.estimated_time
    );