CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_customer_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute'  
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.customer_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.CLEAN_SCH.CUSTOMER AS target
        USING (
            SELECT 
                CUSTOMERID::STRING AS CUSTOMER_ID,
                NAME::STRING AS NAME,
                MOBILE::STRING AS MOBILE,
                EMAIL::STRING AS EMAIL,
                LOGINBYUSING::STRING AS LOGIN_BY_USING,
                GENDER::STRING AS GENDER,
                TRY_TO_DATE(DOB, 'YYYY-MM-DD') AS DOB,                     
                TRY_TO_DATE(ANNIVERSARY, 'YYYY-MM-DD') AS ANNIVERSARY,     
                PREFERENCES::STRING AS PREFERENCES,
                TRY_TO_TIMESTAMP_TZ(CREATEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS CREATED_DT,  
                TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6') AS MODIFIED_DT, 
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            FROM mealsmart_db.STAGE_SCH.CUSTOMER_STM
        ) AS source
        ON target.CUSTOMER_ID = source.CUSTOMER_ID
        WHEN MATCHED THEN
            UPDATE SET 
                target.NAME = source.NAME,
                target.MOBILE = source.MOBILE,
                target.EMAIL = source.EMAIL,
                target.LOGIN_BY_USING = source.LOGIN_BY_USING,
                target.GENDER = source.GENDER,
                target.DOB = source.DOB,
                target.ANNIVERSARY = source.ANNIVERSARY,
                target.PREFERENCES = source.PREFERENCES,
                target.CREATED_DT = source.CREATED_DT,
                target.MODIFIED_DT = source.MODIFIED_DT,
                target._STG_FILE_NAME = source._STG_FILE_NAME,
                target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
                target._STG_FILE_MD5 = source._STG_FILE_MD5,
                target._COPY_DATA_TS = source._COPY_DATA_TS
        WHEN NOT MATCHED THEN
            INSERT (
                CUSTOMER_ID,
                NAME,
                MOBILE,
                EMAIL,
                LOGIN_BY_USING,
                GENDER,
                DOB,
                ANNIVERSARY,
                PREFERENCES,
                CREATED_DT,
                MODIFIED_DT,
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                source.CUSTOMER_ID,
                source.NAME,
                source.MOBILE,
                source.EMAIL,
                source.LOGIN_BY_USING,
                source.GENDER,
                source.DOB,
                source.ANNIVERSARY,
                source.PREFERENCES,
                source.CREATED_DT,
                source.MODIFIED_DT,
                source._STG_FILE_NAME,
                source._STG_FILE_LOAD_TS,
                source._STG_FILE_MD5,
                source._COPY_DATA_TS
            );

    
            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_customer_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.customer_stm')
        -- Perform the merge operation
        AS MERGE INTO 
    mealsmart_db.CONSUMPTION_SCH.CUSTOMER_DIM AS target
USING 
    mealsmart_db.CLEAN_SCH.CUSTOMER_STM AS source
ON 
    target.CUSTOMER_ID = source.CUSTOMER_ID AND
    target.NAME = source.NAME AND
    target.MOBILE = source.MOBILE AND
    target.EMAIL = source.EMAIL AND
    target.LOGIN_BY_USING = source.LOGIN_BY_USING AND
    target.GENDER = source.GENDER AND
    target.DOB = source.DOB AND
    target.ANNIVERSARY = source.ANNIVERSARY AND
    target.PREFERENCES = source.PREFERENCES
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
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES))),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_HK,
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.CUSTOMER_ID, source.NAME, source.MOBILE, 
            source.EMAIL, source.LOGIN_BY_USING, source.GENDER, source.DOB, 
            source.ANNIVERSARY, source.PREFERENCES))),
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );