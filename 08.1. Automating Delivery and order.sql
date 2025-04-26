CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_delivery_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.delivery_stm') 
        -- Perform the merge operation
        AS MERGE INTO 
    mealsmart_db.clean_sch.delivery AS target
USING 
    mealsmart_db.stage_sch.delivery_stm AS source
ON 
    target.delivery_id = TO_NUMBER(source.deliveryid) and
    target.order_id_fk = TO_NUMBER(source.orderid) and
    target.delivery_agent_id_fk = TO_NUMBER(source.deliveryagentid)
WHEN MATCHED THEN
    -- Update the existing record with the latest data
    UPDATE SET
        delivery_status = source.deliverystatus,
        estimated_time = source.estimatedtime,
        customer_address_id_fk = TO_NUMBER(source.addressid),
        delivery_date = TO_TIMESTAMP(source.deliverydate),
        created_date = TO_TIMESTAMP(source.createddate),
        modified_date = TO_TIMESTAMP(source.modifieddate),
        _stg_file_name = source._stg_file_name,
        _stg_file_load_ts = source._stg_file_load_ts,
        _stg_file_md5 = source._stg_file_md5,
        _copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    -- Insert new record if no match is found
    INSERT (
        delivery_id,
        order_id_fk,
        delivery_agent_id_fk,
        delivery_status,
        estimated_time,
        customer_address_id_fk,
        delivery_date,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        TO_NUMBER(source.deliveryid),
        TO_NUMBER(source.orderid),
        TO_NUMBER(source.deliveryagentid),
        source.deliverystatus,
        source.estimatedtime,
        TO_NUMBER(source.addressid),
        TO_TIMESTAMP(source.deliverydate),
        TO_TIMESTAMP(source.createddate),
        TO_TIMESTAMP(source.modifieddate),
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );
                
            

  CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_orders_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.orders_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.CLEAN_SCH.ORDERS AS target
        USING mealsmart_db.STAGE_SCH.ORDERS_STM AS source
            ON target.ORDER_ID = TRY_TO_NUMBER(source.ORDERID) -- Match based on ORDER_ID
        WHEN MATCHED THEN
            -- Update existing records
            UPDATE SET
                TOTAL_AMOUNT = TRY_TO_DECIMAL(source.TOTALAMOUNT),
                STATUS = source.STATUS,
                PAYMENT_METHOD = source.PAYMENTMETHOD,
                MODIFIED_DT = TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
                _STG_FILE_NAME = source._STG_FILE_NAME,
                _STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
                _STG_FILE_MD5 = source._STG_FILE_MD5,
                _COPY_DATA_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            -- Insert new records
            INSERT (
                ORDER_ID,
                CUSTOMER_ID_FK,
                RESTAURANT_ID_FK,
                ORDER_DATE,
                TOTAL_AMOUNT,
                STATUS,
                PAYMENT_METHOD,
                CREATED_DT,
                MODIFIED_DT,
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                TRY_TO_NUMBER(source.ORDERID),
                TRY_TO_NUMBER(source.CUSTOMERID),
                TRY_TO_NUMBER(source.RESTAURANTID),
                TRY_TO_TIMESTAMP(source.ORDERDATE),
                TRY_TO_DECIMAL(source.TOTALAMOUNT),
                source.STATUS,
                source.PAYMENTMETHOD,
                TRY_TO_TIMESTAMP_TZ(source.CREATEDDATE),
                TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
                source._STG_FILE_NAME,
                source._STG_FILE_LOAD_TS,
                source._STG_FILE_MD5,
                CURRENT_TIMESTAMP
            );


CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_order_item_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.orderitem_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.order_item AS target
        USING mealsmart_db.stage_sch.orderitem_stm AS source
        ON  
            target.order_item_id = source.orderitemid and
            target.order_id_fk = source.orderid and
            target.menu_id_fk = source.menuid
        WHEN MATCHED THEN
            -- Update the existing record with new data
            UPDATE SET 
                target.quantity = source.quantity,
                target.price = source.price,
                target.subtotal = source.subtotal,
                target.created_dt = source.createddate,
                target.modified_dt = source.modifieddate,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            -- Insert new record if no match is found
            INSERT (
                order_item_id,
                order_id_fk,
                menu_id_fk,
                quantity,
                price,
                subtotal,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                source.orderitemid,
                source.orderid,
                source.menuid,
                source.quantity,
                source.price,
                source.subtotal,
                source.createddate,
                source.modifieddate,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                CURRENT_TIMESTAMP()
            );