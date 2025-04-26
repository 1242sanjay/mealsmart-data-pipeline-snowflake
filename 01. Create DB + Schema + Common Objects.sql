-- switch role to sysadmin role
use role sysadmin;

-- create a warehouse if not exist 
create warehouse if not exists adhoc_wh
     comment = 'This is the adhoc-wh'
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- create database/schema if does not exist
create database if not exists mealsmart_db;
use database mealsmart_db;
create schema if not exists stage_sch;
create schema if not exists clean_sch;
create schema if not exists consumption_sch;
create schema if not exists common;

use schema stage_sch;

-- create file format to process the csv file
create file format if not exists mealsmart_db.stage_sch.csv_file_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    field_optionally_enclosed_by = '\042'
    null_if = ('\\N')
;

/*
-- storage integration
create or replace storage integration aws_S3_integration
type = external_stage
storage_provider = 'S3'
enabled = true
storage_aws_role_arn = 'arn:aws:iam::396913714134:role/aws-snowflake'
storage_allowed_locations = ('s3://aws-snow-bucket/');
*/

use role accountadmin;
grant usage on integration aws_s3_integration to role sysadmin;
use role sysadmin;

show integrations;
desc storage integration aws_s3_integration;

-- create stage 
create or replace stage mealsmart_db.stage_sch.mealsmart_csv_stg
url = 's3://aws-snow-bucket/mealsmart/'
storage_integration = aws_s3_integration
comment = 'This is the snowflake internal stage'
;


-- create policies in common schema
-- create tag 
create or replace tag mealsmart_db.common.pii_policy_tag
allowed_values 'PII', 'PRICE', 'SENSITIVE', 'EMAIL'
comment = 'This is PII policy tag object'
;

-- create masking policy
create or replace masking policy mealsmart_db.common.pii_masking_policy as (pii_text string)
returns string -> to_varchar('*** PII ***')
;

create or replace masking policy mealsmart_db.common.email_masking_policy as (email_text string)
returns string -> to_varchar('*** Email ***')
;

create or replace masking policy mealsmart_db.common.pii_masking_policy as (phone_text string)
returns string -> to_varchar('*** Phone ***')
;



