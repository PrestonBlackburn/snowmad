<div align="center">
  <img src="https://drive.google.com/uc?export=view&id=1MS6Y-2orri9vxJLujNI6P5zecMvRfL9r"><br>
</div>


# snowmad: a snowflake transcription library  
[![PyPI Latest Release](https://img.shields.io/pypi/v/snowmad.svg)](https://pypi.org/project/snowmad/)
[![Package Status](https://img.shields.io/pypi/status/snowmad.svg)](https://pypi.org/project/snowmad/)
[![License](https://img.shields.io/pypi/l/snowmad.svg)](https://github.com/PrestonBlackburn/snowmad/blob/main/LICENSE)


## What is it?

**Snowmad** is a Python package that provides utilities to transcribe snowflake objects. Currently transcribing objects form Snowflake to Terraform and transcribing objects from Snowflake to another Snowflake account is the primary focus of this package. The goal of this package is to cut down on writing one off scripts or manually wirting ddl for copying snowflake objects. 


## Main Features
- **Terraform**
    - Transcribe snowflake users, roles, and role grants to terraform format
    - Currently terraform can import state, but cannot generate a config file from the imported state
    - To be used with snowflake provider (snowflake-labs/snowflake)
- **Snowflake Accounts**  
    - Copy databse objects ddl from one Snowflake account to another
    - Copy account level objects from one account to another including: warehouses, users, roles, and grants


## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/PrestonBlackburn/snowmad  

Binary installers for the latest released version are available at the [Python
Package Index (PyPI)](https://pypi.org/project/pandas)  
<br/>
```sh
# PyPI
pip install snowmad
```


## Dependencies
- pandas - for data handling
- snowflake.connector (with pandas addon, "snowflake-connector-python[pandas]") - for connecting to snowflake accounts and loading object data to dataframes
- configparser - for handling snowflake account input data
- cryptography - for accessing accounts via rsa key
- snowflake connector 

## License
[MIT](LICENSE)


## Documentation
See the current documentation on the [wiki page](https://github.com/PrestonBlackburn/snowmad/wiki)


## Outputs
- roles text file
    - contains snowflake roles in terraform "snowflake_role" resource format
- users text file
    - contains snowflake users in terraform "snowflake_user" resource format
- grants text file
    - contains snowflake grants for roles in terraform "snowflake_role_grants" resource format


