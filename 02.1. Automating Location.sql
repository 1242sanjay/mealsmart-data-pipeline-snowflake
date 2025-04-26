CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_restaurant_location_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute'  
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.location_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.restaurant_location AS target
        USING (
            SELECT 
                CAST(LocationID AS NUMBER) AS Location_ID,
                CAST(City AS STRING) AS City,
                CASE 
                    WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
                    ELSE CAST(State AS STRING)
                END AS State,
                -- State Code Mapping
                CASE 
                    WHEN State = 'Delhi' THEN 'DL'
                    WHEN State = 'Maharashtra' THEN 'MH'
                    WHEN State = 'Uttar Pradesh' THEN 'UP'
                    WHEN State = 'Gujarat' THEN 'GJ'
                    WHEN State = 'Rajasthan' THEN 'RJ'
                    WHEN State = 'Kerala' THEN 'KL'
                    WHEN State = 'Punjab' THEN 'PB'
                    WHEN State = 'Karnataka' THEN 'KA'
                    WHEN State = 'Madhya Pradesh' THEN 'MP'
                    WHEN State = 'Odisha' THEN 'OR'
                    WHEN State = 'Chandigarh' THEN 'CH'
                    WHEN State = 'West Bengal' THEN 'WB'
                    WHEN State = 'Sikkim' THEN 'SK'
                    WHEN State = 'Andhra Pradesh' THEN 'AP'
                    WHEN State = 'Assam' THEN 'AS'
                    WHEN State = 'Jammu and Kashmir' THEN 'JK'
                    WHEN State = 'Puducherry' THEN 'PY'
                    WHEN State = 'Uttarakhand' THEN 'UK'
                    WHEN State = 'Himachal Pradesh' THEN 'HP'
                    WHEN State = 'Tamil Nadu' THEN 'TN'
                    WHEN State = 'Goa' THEN 'GA'
                    WHEN State = 'Telangana' THEN 'TG'
                    WHEN State = 'Chhattisgarh' THEN 'CG'
                    WHEN State = 'Jharkhand' THEN 'JH'
                    WHEN State = 'Bihar' THEN 'BR'
                    ELSE NULL
                END AS state_code,
                CASE 
                    WHEN State IN ('Delhi', 'Chandigarh', 'Puducherry', 'Jammu and Kashmir') THEN 'Y'
                    ELSE 'N'
                END AS is_union_territory,
                CASE 
                    WHEN (State = 'Delhi' AND City = 'New Delhi') THEN TRUE
                    WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
                    -- Other conditions for capital cities
                    ELSE FALSE
                END AS capital_city_flag,
                CASE 
                    WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad') THEN 'Tier-1'
                    WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara', 'Coimbatore', 
                                  'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati', 'Chandigarh') THEN 'Tier-2'
                    ELSE 'Tier-3'
                END AS city_tier,
                CAST(ZipCode AS STRING) AS Zip_Code,
                CAST(ActiveFlag AS STRING) AS Active_Flag,
                TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS created_ts,
                TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modified_ts,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                CURRENT_TIMESTAMP AS _copy_data_ts
            FROM mealsmart_db.stage_sch.location_stm
        ) AS source
        ON target.Location_ID = source.Location_ID
        WHEN MATCHED AND (
            target.City != source.City OR
            target.State != source.State OR
            target.state_code != source.state_code OR
            target.is_union_territory != source.is_union_territory OR
            target.capital_city_flag != source.capital_city_flag OR
            target.city_tier != source.city_tier OR
            target.Zip_Code != source.Zip_Code OR
            target.Active_Flag != source.Active_Flag OR
            target.modified_ts != source.modified_ts
        ) THEN 
            UPDATE SET 
                target.City = source.City,
                target.State = source.State,
                target.state_code = source.state_code,
                target.is_union_territory = source.is_union_territory,
                target.capital_city_flag = source.capital_city_flag,
                target.city_tier = source.city_tier,
                target.Zip_Code = source.Zip_Code,
                target.Active_Flag = source.Active_Flag,
                target.modified_ts = source.modified_ts,
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            INSERT (
                Location_ID,
                City,
                State,
                state_code,
                is_union_territory,
                capital_city_flag,
                city_tier,
                Zip_Code,
                Active_Flag,
                created_ts,
                modified_ts,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                source.Location_ID,
                source.City,
                source.State,
                source.state_code,
                source.is_union_territory,
                source.capital_city_flag,
                source.city_tier,
                source.Zip_Code,
                source.Active_Flag,
                source.created_ts,
                source.modified_ts,
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                source._copy_data_ts
            );

            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_restaurant_location_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.restaurant_location_stm')
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM AS target
        USING mealsmart_db.CLEAN_SCH.RESTAURANT_LOCATION_STM AS source
        ON 
            target.LOCATION_ID = source.LOCATION_ID and 
            target.ACTIVE_FLAG = source.ACTIVE_FLAG
            WHEN MATCHED 
                AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.EFF_END_DT = CURRENT_TIMESTAMP(),
                target.CURRENT_FLAG = FALSE
            WHEN NOT MATCHED 
                AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
            THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_LOCATION_HK,
                LOCATION_ID,
                CITY,
                STATE,
                STATE_CODE,
                IS_UNION_TERRITORY,
                CAPITAL_CITY_FLAG,
                CITY_TIER,
                ZIP_CODE,
                ACTIVE_FLAG,
                EFF_START_DT,
                EFF_END_DT,
                CURRENT_FLAG
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
                source.LOCATION_ID,
                source.CITY,
                source.STATE,
                source.STATE_CODE,
                source.IS_UNION_TERRITORY,
                source.CAPITAL_CITY_FLAG,
                source.CITY_TIER,
                source.ZIP_CODE,
                source.ACTIVE_FLAG,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            )
            WHEN NOT MATCHED AND 
            source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                RESTAURANT_LOCATION_HK,
                LOCATION_ID,
                CITY,
                STATE,
                STATE_CODE,
                IS_UNION_TERRITORY,
                CAPITAL_CITY_FLAG,
                CITY_TIER,
                ZIP_CODE,
                ACTIVE_FLAG,
                EFF_START_DT,
                EFF_END_DT,
                CURRENT_FLAG
            )
            VALUES (
                hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIP_CODE))),
                source.LOCATION_ID,
                source.CITY,
                source.STATE,
                source.STATE_CODE,
                source.IS_UNION_TERRITORY,
                source.CAPITAL_CITY_FLAG,
                source.CITY_TIER,
                source.ZIP_CODE,
                source.ACTIVE_FLAG,
                CURRENT_TIMESTAMP(),
                NULL,
                TRUE
            );