# snowmad
A user management library for transcribing Snowflake roles and user permissions to terraform format

## Purpose
- Transcribe snowflake users, roles, and role grants to terraform format
- Currently terraform can import state, but cannot generate a config file from the imported state
- To be used with snowflake provider (snowflake-labs/snowflake)

## Dependencies
- pandas
- snowflake.connector 
- configparser

## Outputs
- roles text file
    - contains snowflake roles in terraform "snowflake_role" resource format
- users text file
    - contains snowflake users in terraform "snowflake_user" resource format
- grants text file
    - contains snowflake grants for roles in terraform "snowflake_role_grants" resource format