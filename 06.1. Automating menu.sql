CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_menu_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.menu_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.menu AS target
        USING (
            SELECT 
                TRY_CAST(menuid AS INT) AS Menu_ID,
                TRY_CAST(restaurantid AS INT) AS Restaurant_ID_FK,
                TRIM(itemname) AS Item_Name,
                TRIM(description) AS Description,
                TRY_CAST(price AS DECIMAL(10, 2)) AS Price,
                TRIM(category) AS Category,
                CASE 
                    WHEN LOWER(availability) = 'true' THEN TRUE
                    WHEN LOWER(availability) = 'false' THEN FALSE
                    ELSE NULL
                END AS Availability,
                TRIM(itemtype) AS Item_Type,
                TRY_CAST(createddate AS TIMESTAMP_NTZ) AS Created_dt,  -- Renamed column
                TRY_CAST(modifieddate AS TIMESTAMP_NTZ) AS Modified_dt, -- Renamed column
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            FROM mealsmart_db.stage_sch.menu_stm
        ) AS source
        ON target.Menu_ID = source.Menu_ID
        WHEN MATCHED THEN
            UPDATE SET
                Restaurant_ID_FK = source.Restaurant_ID_FK,
                Item_Name = source.Item_Name,
                Description = source.Description,
                Price = source.Price,
                Category = source.Category,
                Availability = source.Availability,
                Item_Type = source.Item_Type,
                Created_dt = source.Created_dt,  
                Modified_dt = source.Modified_dt,  
                _STG_FILE_NAME = source._stg_file_name,
                _STG_FILE_LOAD_TS = source._stg_file_load_ts,
                _STG_FILE_MD5 = source._stg_file_md5,
                _COPY_DATA_TS = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                Created_dt, 
                Modified_dt,  
                _STG_FILE_NAME,
                _STG_FILE_LOAD_TS,
                _STG_FILE_MD5,
                _COPY_DATA_TS
            )
            VALUES (
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                source.Created_dt,  
                source.Modified_dt,  
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                CURRENT_TIMESTAMP
            );
        
        
            
            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_menu_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.menu_stm')
        -- Perform the merge operation
        AS MERGE INTO 
            mealsmart_db.consumption_sch.MENU_DIM AS target
        USING 
            mealsmart_db.CLEAN_SCH.MENU_STM AS source
        ON 
            target.Menu_ID = source.Menu_ID AND
            target.Restaurant_ID_FK = source.Restaurant_ID_FK AND
            target.Item_Name = source.Item_Name AND
            target.Description = source.Description AND
            target.Price = source.Price AND
            target.Category = source.Category AND
            target.Availability = source.Availability AND
            target.Item_Type = source.Item_Type
        WHEN MATCHED 
            AND source.METADATA$ACTION = 'DELETE' 
            AND source.METADATA$ISUPDATE = 'TRUE' THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DATE = CURRENT_TIMESTAMP(),
                target.IS_CURRENT = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = 'INSERT' 
            AND source.METADATA$ISUPDATE = 'TRUE' THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                Menu_Dim_HK,               -- Hash key
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
                    source.Item_Name, source.Description, source.Price, 
                    source.Category, source.Availability, source.Item_Type))),  -- Hash key
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                CURRENT_TIMESTAMP(),       -- Effective start date
                NULL,                      -- Effective end date (NULL for current record)
                TRUE                       -- IS_CURRENT = TRUE for new record
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = 'INSERT' 
            AND source.METADATA$ISUPDATE = 'FALSE' THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                Menu_Dim_HK,               -- Hash key
                Menu_ID,
                Restaurant_ID_FK,
                Item_Name,
                Description,
                Price,
                Category,
                Availability,
                Item_Type,
                EFF_START_DATE,
                EFF_END_DATE,
                IS_CURRENT
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.Menu_ID, source.Restaurant_ID_FK, 
                    source.Item_Name, source.Description, source.Price, 
                    source.Category, source.Availability, source.Item_Type))),  -- Hash key
                source.Menu_ID,
                source.Restaurant_ID_FK,
                source.Item_Name,
                source.Description,
                source.Price,
                source.Category,
                source.Availability,
                source.Item_Type,
                CURRENT_TIMESTAMP(),       -- Effective start date
                NULL,                      -- Effective end date (NULL for current record)
                TRUE                       -- IS_CURRENT = TRUE for new record
            );
