CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_customeraddress_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.customeraddress_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.customer_address AS clean
USING (
    SELECT 
        CAST(addressid AS INT) AS address_id,
        CAST(customerid AS INT) AS customer_id_fk,
        flatno AS flat_no,
        houseno AS house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primaryflag AS primary_flag,
        addresstype AS address_type,
        TRY_TO_TIMESTAMP_TZ(createddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS created_date,
        TRY_TO_TIMESTAMP_TZ(modifieddate, 'YYYY-MM-DD"T"HH24:MI:SS') AS modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM mealsmart_db.stage_sch.customeraddress_stm 
) AS stage
ON clean.address_id = stage.address_id
-- Insert new records
WHEN NOT MATCHED THEN
    INSERT (
        address_id,
        customer_id_fk,
        flat_no,
        house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag,
        address_type,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        stage.address_id,
        stage.customer_id_fk,
        stage.flat_no,
        stage.house_no,
        stage.floor,
        stage.building,
        stage.landmark,
        stage.locality,
        stage.city,
        stage.state,
        stage.pincode,
        stage.coordinates,
        stage.primary_flag,
        stage.address_type,
        stage.created_date,
        stage.modified_date,
        stage._stg_file_name,
        stage._stg_file_load_ts,
        stage._stg_file_md5,
        stage._copy_data_ts
    )
-- Update existing records
WHEN MATCHED THEN
    UPDATE SET
        clean.flat_no = stage.flat_no,
        clean.house_no = stage.house_no,
        clean.floor = stage.floor,
        clean.building = stage.building,
        clean.landmark = stage.landmark,
        clean.locality = stage.locality,
        clean.city = stage.city,
        clean.state = stage.state,
        clean.pincode = stage.pincode,
        clean.coordinates = stage.coordinates,
        clean.primary_flag = stage.primary_flag,
        clean.address_type = stage.address_type,
        clean.created_date = stage.created_date,
        clean.modified_date = stage.modified_date,
        clean._stg_file_name = stage._stg_file_name,
        clean._stg_file_load_ts = stage._stg_file_load_ts,
        clean._stg_file_md5 = stage._stg_file_md5,
        clean._copy_data_ts = stage._copy_data_ts;


    
            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_customer_address_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.customer_address_stm')
        -- Perform the merge operation
        AS MERGE INTO 
    mealsmart_db.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM AS target
USING 
    mealsmart_db.CLEAN_SCH.CUSTOMER_ADDRESS_STM AS source
ON 
    target.ADDRESS_ID = source.ADDRESS_ID AND
    target.CUSTOMER_ID_FK = source.CUSTOMER_ID_FK AND
    target.FLAT_NO = source.FLAT_NO AND
    target.HOUSE_NO = source.HOUSE_NO AND
    target.FLOOR = source.FLOOR AND
    target.BUILDING = source.BUILDING AND
    target.LANDMARK = source.LANDMARK AND
    target.LOCALITY = source.LOCALITY AND
    target.CITY = source.CITY AND
    target.STATE = source.STATE AND
    target.PINCODE = source.PINCODE AND
    target.COORDINATES = source.COORDINATES AND
    target.PRIMARY_FLAG = source.PRIMARY_FLAG AND
    target.ADDRESS_TYPE = source.ADDRESS_TYPE
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
        CUSTOMER_ADDRESS_HK,
        ADDRESS_ID,
        CUSTOMER_ID_FK,
        FLAT_NO,
        HOUSE_NO,
        FLOOR,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        PINCODE,
        COORDINATES,
        PRIMARY_FLAG,
        ADDRESS_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, 
            source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, 
            source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
            source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
        source.ADDRESS_ID,
        source.CUSTOMER_ID_FK,
        source.FLAT_NO,
        source.HOUSE_NO,
        source.FLOOR,
        source.BUILDING,
        source.LANDMARK,
        source.LOCALITY,
        source.CITY,
        source.STATE,
        source.PINCODE,
        source.COORDINATES,
        source.PRIMARY_FLAG,
        source.ADDRESS_TYPE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_ADDRESS_HK,
        ADDRESS_ID,
        CUSTOMER_ID_FK,
        FLAT_NO,
        HOUSE_NO,
        FLOOR,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        PINCODE,
        COORDINATES,
        PRIMARY_FLAG,
        ADDRESS_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.FLAT_NO, 
            source.HOUSE_NO, source.FLOOR, source.BUILDING, source.LANDMARK, 
            source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
            source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
        source.ADDRESS_ID,
        source.CUSTOMER_ID_FK,
        source.FLAT_NO,
        source.HOUSE_NO,
        source.FLOOR,
        source.BUILDING,
        source.LANDMARK,
        source.LOCALITY,
        source.CITY,
        source.STATE,
        source.PINCODE,
        source.COORDINATES,
        source.PRIMARY_FLAG,
        source.ADDRESS_TYPE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );