CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_restaurant_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute'  
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.restaurant_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.restaurant AS target
        USING (
            SELECT 
                try_cast(restaurantid AS number) AS restaurant_id,
                try_cast(name AS string) AS name,
                try_cast(cuisinetype AS string) AS cuisine_type,
                try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
                try_cast(restaurant_phone AS string) AS restaurant_phone,
                try_cast(operatinghours AS string) AS operating_hours,
                try_cast(locationid AS number) AS location_id_fk,
                try_cast(activeflag AS string) AS active_flag,
                try_cast(openstatus AS string) AS open_status,
                try_cast(locality AS string) AS locality,
                try_cast(restaurant_address AS string) AS restaurant_address,
                try_cast(latitude AS number(9, 6)) AS latitude,
                try_cast(longitude AS number(9, 6)) AS longitude,
                try_to_timestamp_ntz(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
                try_to_timestamp_ntz(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5
            FROM 
                mealsmart_db.stage_sch.restaurant_stm
        ) AS source
        ON target.restaurant_id = source.restaurant_id
        WHEN MATCHED THEN 
            UPDATE SET 
                target.name = source.name,
                target.cuisine_type = source.cuisine_type,
                target.pricing_for_two = source.pricing_for_two,
                target.restaurant_phone = source.restaurant_phone,
                target.operating_hours = source.operating_hours,
                target.location_id_fk = source.location_id_fk,
                target.active_flag = source.active_flag,
                target.open_status = source.open_status,
                target.locality = source.locality,
                target.restaurant_address = source.restaurant_address,
                target.latitude = source.latitude,
                target.longitude = source.longitude,
                target.created_dt = source.created_dt,
                target.modified_dt = source.modified_dt,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5
        WHEN NOT MATCHED THEN 
            INSERT (
                restaurant_id,
                name,
                cuisine_type,
                pricing_for_two,
                restaurant_phone,
                operating_hours,
                location_id_fk,
                active_flag,
                open_status,
                locality,
                restaurant_address,
                latitude,
                longitude,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5
            )
            VALUES (
                source.restaurant_id,
                source.name,
                source.cuisine_type,
                source.pricing_for_two,
                source.restaurant_phone,
                source.operating_hours,
                source.location_id_fk,
                source.active_flag,
                source.open_status,
                source.locality,
                source.restaurant_address,
                source.latitude,
                source.longitude,
                source.created_dt,
                source.modified_dt,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5
            );

    
            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_restaurant_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.restaurant_stm')
        -- Perform the merge operation
        AS MERGE INTO 
    mealsmart_db.CONSUMPTION_SCH.RESTAURANT_DIM AS target
USING 
    mealsmart_db.CLEAN_SCH.RESTAURANT_STM AS source
ON 
    target.RESTAURANT_ID = source.RESTAURANT_ID AND 
    target.NAME = source.NAME AND 
    target.CUISINE_TYPE = source.CUISINE_TYPE AND 
    target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND 
    target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND 
    target.OPERATING_HOURS = source.OPERATING_HOURS AND 
    target.LOCATION_ID_FK = source.LOCATION_ID_FK AND 
    target.ACTIVE_FLAG = source.ACTIVE_FLAG AND 
    target.OPEN_STATUS = source.OPEN_STATUS AND 
    target.LOCALITY = source.LOCALITY AND 
    target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND 
    target.LATITUDE = source.LATITUDE AND 
    target.LONGITUDE = source.LONGITUDE
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );