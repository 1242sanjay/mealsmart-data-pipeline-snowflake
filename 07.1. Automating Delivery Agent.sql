CREATE OR REPLACE TASK mealsmart_db.stage_sch.stg_delivery_agent_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.stage_sch.deliveryagent_stm') 
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.clean_sch.delivery_agent AS target
        USING mealsmart_db.stage_sch.deliveryagent_stm AS source
        ON target.delivery_agent_id = source.deliveryagentid
        WHEN MATCHED THEN
            UPDATE SET
                target.phone = source.phone,
                target.vehicle_type = source.vehicletype,
                target.location_id_fk = TRY_TO_NUMBER(source.locationid),
                target.status = source.status,
                target.gender = source.gender,
                target.rating = TRY_TO_DECIMAL(source.rating,4,2),
                target.created_dt = TRY_TO_TIMESTAMP(source.createddate),
                target.modified_dt = TRY_TO_TIMESTAMP(source.modifieddate),
                target._stg_file_name = source._stg_file_name,
                target._stg_file_load_ts = source._stg_file_load_ts,
                target._stg_file_md5 = source._stg_file_md5,
                target._copy_data_ts = source._copy_data_ts
        WHEN NOT MATCHED THEN
            INSERT (
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                created_dt,
                modified_dt,
                _stg_file_name,
                _stg_file_load_ts,
                _stg_file_md5,
                _copy_data_ts
            )
            VALUES (
                TRY_TO_NUMBER(source.deliveryagentid),
                source.name,
                source.phone,
                source.vehicletype,
                TRY_TO_NUMBER(source.locationid),
                source.status,
                source.gender,
                TRY_TO_NUMBER(source.rating),
                TRY_TO_TIMESTAMP(source.createddate),
                TRY_TO_TIMESTAMP(source.modifieddate),
                source._stg_file_name,
                source._stg_file_load_ts,
                source._stg_file_md5,
                CURRENT_TIMESTAMP()
            );
                
            
            
CREATE OR REPLACE TASK mealsmart_db.clean_sch.cln_delivery_agent_tsk
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 minute' 
    -- Check if the stream has unprocessed data
    WHEN SYSTEM$STREAM_HAS_DATA('mealsmart_db.clean_sch.delivery_agent_stm')
        -- Perform the merge operation
        AS MERGE INTO mealsmart_db.consumption_sch.delivery_agent_dim AS target
        USING mealsmart_db.CLEAN_SCH.delivery_agent_stm AS source
        ON 
            target.delivery_agent_id = source.delivery_agent_id AND
            target.name = source.name AND
            target.phone = source.phone AND
            target.vehicle_type = source.vehicle_type AND
            target.location_id_fk = source.location_id_fk AND
            target.status = source.status AND
            target.gender = source.gender AND
            target.rating = source.rating
        WHEN MATCHED 
            AND source.METADATA$ACTION = 'DELETE' 
            AND source.METADATA$ISUPDATE = 'TRUE' THEN
            -- Update the existing record to close its validity period
            UPDATE SET 
                target.eff_end_date = CURRENT_TIMESTAMP,
                target.is_current = FALSE
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = 'INSERT' 
            AND source.METADATA$ISUPDATE = 'TRUE' THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                delivery_agent_hk,        -- Hash key
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                eff_start_date,
                eff_end_date,
                is_current
            )
            VALUES (
                hash(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.phone, 
                    source.vehicle_type, source.location_id_fk, source.status, 
                    source.gender, source.rating))), -- Hash key
                delivery_agent_id,
                source.name,
                source.phone,
                source.vehicle_type,
                location_id_fk,
                source.status,
                source.gender,
                source.rating,
                CURRENT_TIMESTAMP,       -- Effective start date
                NULL,                    -- Effective end date (NULL for current record)
                TRUE                    -- IS_CURRENT = TRUE for new record
            )
        WHEN NOT MATCHED 
            AND source.METADATA$ACTION = 'INSERT' 
            AND source.METADATA$ISUPDATE = 'FALSE' THEN
            -- Insert new record with current data and new effective start date
            INSERT (
                delivery_agent_hk,        -- Hash key
                delivery_agent_id,
                name,
                phone,
                vehicle_type,
                location_id_fk,
                status,
                gender,
                rating,
                eff_start_date,
                eff_end_date,
                is_current
            )
            VALUES (
                hash(SHA1_HEX(CONCAT(source.delivery_agent_id, source.name, source.phone, 
                    source.vehicle_type, source.location_id_fk, source.status,
                    source.gender, source.rating))), -- Hash key
                source.delivery_agent_id,
                source.name,
                source.phone,
                source.vehicle_type,
                source.location_id_fk,
                source.status,
                source.gender,
                source.rating,
                CURRENT_TIMESTAMP,       -- Effective start date
                NULL,                    -- Effective end date (NULL for current record)
                TRUE                   -- IS_CURRENT = TRUE for new record
            );