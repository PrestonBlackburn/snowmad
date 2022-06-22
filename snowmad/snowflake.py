import pandas as pd
import snowflake.connector 
import configparser
import re

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization


def parse_credentials(config_file, config_name, conn_type):
    """ helper function to connect to source and target snowflake accounts"""
    
    credentials = configparser.ConfigParser()
    credentials.read(config_file)
    
    user = credentials[config_name]['user']
    account = credentials[config_name]['account']
    warehouse = credentials[config_name]['warehouse']
    
    if conn_type == 'password':
        password = credentials[config_name]['password']

        conn = snowflake.connector.connect(
                user = user,
                password = password,
                account = account,
                warehouse = warehouse
            )

        cur = conn.cursor()
    
    if conn_type == 'private_key':
        pkb_key_path = credentials[config_name]['private_key']
        
        with open(pkb_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())
        
        
        conn = snowflake.connector.connect(
            user = user,
            account = account,
            private_key=pkb,
            warehouse = warehouse
        )
        
        cur = conn.cursor()

    return conn, cur, account


def execute_sql_list(sql_list, cursor, return_sql = False, return_errors = True):
    """ Execute sql statements and skip any that can't be executed """
    
    # todo:
    # handle exceptions better
    # always give option to skip objects that can't be created
    # put return sql option here as well
    
    for sql in sql_list:
        try:
            if return_sql:
                print("Executing: ", sql)
                cursor.execute(sql)
            else:
                cursor.execute(sql)

        except snowflake.connector.errors.ProgrammingError as e:
            if return_errors:
                print(e)
                print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            else:
                pass

            continue

        except Exception as error:
            if return_errors:
                print(error)
                print("Could not create grants for users")
            else:
                pass
            continue
    
    
def fetch_data_df(sql, connection):
    # fetch data from source account in a dataframe
    try:
        df = pd.read_sql(sql, connection)

    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

    except Exception as error:
        print(error)
        print(f"fetching data failed for: \n  {sql}")
        
    return df


        
class transcribe_account:
    """
    A class for copying objects from one snowflake account to another

    Attributes:
        config_file : str
            the path of the config file that contains snowflake credentials for two accounts
            currently only password authentication is supported
        source_config_name : str
            the name of the header in the config file for the snowflake source account credentials
        target_config_name : str
            the name of the header in the config file for the snowflake source account credentials
        conn_type_source: str
            source account authentication type. can be 'password' or 'private_key'
        conn_type_target: str
            source account authentication type. can be 'password' or 'private_key
        db_ingore_list: list
            list of database names that should not be replicated
        return_sql: bool
            if true all of the sql statements that are executed will be printed

    """
    
    def __init__(self, 
                 config_file, 
                 source_config_name='snowflake_source_account',
                 target_config_name='snowflake_target_account', 
                 conn_type_source = 'password', 
                 conn_type_target = 'private_key',
                 db_ignore_list = [""],
                 return_sql = True):
        
        self.sql_drop_list = []
        self.db_ignore_list = db_ignore_list
        self.return_sql = return_sql
        
        
        try:
            self.source_conn, self.source_cur, account_source = parse_credentials(config_file,
                                                                                  source_config_name,
                                                                                  conn_type_source)
            print("connected to source account")
            
            self.target_conn, self.target_cur, account_target = parse_credentials(config_file,
                                                                                  target_config_name,
                                                                                  conn_type_target)
            
            print("connected to target account")
            
            

            assert account_source != account_target, f"""Error: Source and Target Accounts Must Be Different: \n
                                                            Source account = {account_source}, Target account = {account_target}"""
        
        except AssertionError as error:
            print(error)
            
        except:
            print("connection to snowflake accounts could not be established")
        
        
    def database_objects(self):
        """ - Reads databases from the source account
            - Creates databases in the target account
            - Outputs a list of sql for dropping objects
        """
        
        sql = 'show databases'
        df_db = fetch_data_df(sql, self.source_conn)
        
        
        # Don't include default snowflake databases:
        df_db = df_db[(df_db['name'] != 'SNOWFLAKE') & (df_db['name'] != 'SNOWFLAKE_SAMPLE_DATA')]
        
        # Don't include shares (origin is other than your account)
        df_db = df_db[df_db['origin'] == ""]
        
        # Don't include databases on the ignore list:
        databases = df_db[~df_db['name'].isin(self.db_ignore_list)]['name'].unique().tolist()
        
        # for dropping dbs
        self.db_drop_sql_list = [f"""DROP DATABASE IF EXISTS "{database}";""" for database in databases]
        self.sql_drop_list += self.db_drop_sql_list
        

        try:
            # Get + execute ddl for all objects in one database
            for database in databases:
                
                try:
                    sql = f"""select get_ddl('database', '{database}', true)"""
                    df_db_ddl = pd.read_sql(sql, self.source_conn)

                    list_of_commands = [x for x in [ re.sub(r"[\n\t]*", "", x) for x in df_db_ddl.iloc[0,0].split(";") ]  if x ]

                    # Ignore specified objects (need to make more robust when I have more time)
                    ignore_objects = ['PROCEDURE', 'FUNCTION', 'STAGE', 'STREAM', 'TASK',
                                      'FILE FORMAT', 'VIEW', 'PIPE', 'MATERIALIZED', 'SECURE',
                                      'RECURSIVE']
                    ignore_objects_lower = [obj.lower() for obj in ignore_objects]
                    ignore_objects += ignore_objects_lower
                    ignore_create_replace = [f"create or replace {obj}" for obj in ignore_objects]
                    ignore_create_replace_upper = [f"CREATE OR REPLACE {obj}" for obj in ignore_objects]
                    ignore_create = [f"create {obj}" for obj in ignore_objects]   
                    ignore_create_upper = [f"CREATE {obj}" for obj in ignore_objects]   
                    misc_ignore_text = [" references ", "MASKING POLICY", "masking policy"]
                    ignore_text = ignore_create + ignore_create_replace + ignore_create_upper + ignore_create_replace_upper + misc_ignore_text


                    list_of_commands_filtered = [ddl for ddl in list_of_commands if all(txt not in ddl for txt in ignore_text)]
                    list_of_commands_filtered = [ddl for ddl in list_of_commands_filtered if ddl.startswith("CREATE") | ddl.startswith("create")]
                    
                    execute_sql_list(list_of_commands_filtered, self.target_cur, return_sql = self.return_sql, return_errors = True)
                    
                    
                except Exception:
                    print(f"Could Not Create: {database}")
                    continue
                
            print("created db objects")

            
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
            
        except Exception as error:
            print(error)
            print("could not create databases and database objects")
        
    
    
    def roles(self):
        """ - Reads roles from the source account
            - Creates roles in the target account
            - Outputs a list of sql for dropping objects
        """
        
        # don't re-create default roles
        sql = """select * from snowflake.account_usage.roles
                    where name not like 'PUBLIC' and
                    name not like 'ACCOUNTADMIN' and
                    name not like 'SECURITYADMIN' and 
                    name not like 'ORGADMIN' and
                    name not like 'USERADMIN' and
                    name not like 'SYSADMIN';"""
        df_roles = fetch_data_df(sql, self.source_conn)
            
            
        roles = df_roles['NAME'].values.tolist()
        self.drop_roles_sql_list = [f"""DROP ROLE IF EXISTS "{role}";""" for role in roles]
        self.sql_drop_list += self.drop_roles_sql_list
        
        roles_sql =  [f"""CREATE OR REPLACE ROLE {role}""" for role in roles]
    
        execute_sql_list(roles_sql, self.target_cur, return_sql = self.return_sql, return_errors = True)
        
      
        
    def users(self):
        """ - Reads users from the source account
            - Creates users in the target account
            - Outputs a list of sql for dropping objects
            - Users created with: name, login_name, display_name, default_role, email
        """
        
        self.user_drop_list = []
        
        ## Ingore default snowflake role and the user who was used to create the account
        sql = """select * from snowflake.account_usage.users
                where name not like 'SNOWFLAKE' and
                created_on not in (SELECT min(created_on) FROM snowflake.account_usage.users);"""
        
        df_users = fetch_data_df(sql, self.source_conn)

        
        names = df_users['NAME'].values.tolist()
        login_names = df_users['LOGIN_NAME'].values.tolist()
        display_names = df_users['DISPLAY_NAME'].values.tolist()
        default_roles = df_users['DEFAULT_ROLE'].values.tolist()
        emails = df_users['EMAIL'].values.tolist()
        
        self.drop_user_sql_list = [f"""DROP USER IF EXISTS "{user}";""" for user in names]
        self.sql_drop_list += self.drop_user_sql_list
        
        user_sql_list = []
        
        # Construct user sql strings
        for i in range(0, len(df_users)):

            name = f'"{names[i]}"'
            password = "'abc123'"

            if login_names[i] != None:
                login_name = f"login_name='{login_names[i]}'"
            else:
                login_name = ""

            if display_names[i] != None:
                display_name = f" display_name='{display_names[i]}'"
            else:
                display_name = ""

            if default_roles[i] != None:   
                default_role = f" default_role={default_roles[i]}"
            else:
                default_role = ""

            if emails[i] != None:
                email = f" email='{emails[i]}'"
            else:
                email = ""

            sql = [f"""CREATE OR REPLACE USER {name} password={password} {login_name} \
                        {display_name} {default_role} {email}"""]

            user_sql_list += sql
            
        execute_sql_list(user_sql_list, self.target_cur, return_sql = self.return_sql, return_errors = True)

        
    
    def warehouses(self):
        """ - Reads warehouses from the source account
            - Creates warehouses in the target account
            - Outputs a list of sql for dropping objects
            - Warehouses created with: name, size
        """
        
        sql = """show warehouses;"""
        df_wh = fetch_data_df(sql, self.source_conn)

        warehouses = df_wh['name'].values.tolist()
        wh_sizes = df_wh['size'].values.tolist()
        
        self.drop_wh_list = [f"""DROP WAREHOUSE "{wh}";""" for wh in warehouses]
        self.sql_drop_list += self.drop_wh_list

        wh_list = [ f"""CREATE OR REPLACE warehouse {wh} warehouse_size='{size}' initially_suspended=true;""" \
                   for wh, size in zip(warehouses, wh_sizes)]
        
        execute_sql_list(wh_list, self.target_cur, return_sql = self.return_sql, return_errors = True)
        

    
    def user_role_grants(self):
        """ - Reads grants from the source account
            - Creates role grants for users in the target account
            - Future grants not supported yet
        """
        
        sql = """select * from snowflake.account_usage.grants_to_users;"""
        df_user_grants  = fetch_data_df(sql, self.source_conn)
        
        
        # only get the grants that still exist
        df_user_grants = df_user_grants[df_user_grants['DELETED_ON'].isnull()]

        roles = df_user_grants['ROLE'].values.tolist()
        users = df_user_grants['GRANTEE_NAME'].values.tolist()
        
        user_role_grant_list = [f"""GRANT ROLE "{role}" TO USER "{user}";""" \
                                for role, user in zip(roles, users)]

        execute_sql_list(user_role_grant_list, self.target_cur, return_sql = self.return_sql, return_errors = True)
        
        
        
    def role_role_grants(self): 
        """ - Reads grants from the source account
            - Creates role grants for roles in the target account
            - Future grants not supported yet
        """
        
        sql = """select * from snowflake.account_usage.grants_to_roles;"""
        df_grants  = fetch_data_df(sql, self.source_conn)
        
        # just looking at roles in this step
        df_role_grants = df_grants[df_grants['GRANTED_ON'] == 'ROLE']
        
        # only get the grants that still exist
        df_role_grants = df_role_grants[df_role_grants ['DELETED_ON'].isnull()]
        
        role_sources = df_role_grants['NAME'].values.tolist()
        role_targets =df_role_grants['GRANTEE_NAME'].values.tolist()
        
        role_role_grant_list = [f"""GRANT ROLE "{role_source}" TO ROLE "{role_target}";""" \
                                    for role_source, role_target in zip(role_sources, role_targets)]
        
        execute_sql_list(role_role_grant_list, self.target_cur, return_sql = self.return_sql, return_errors = True)
        
        
            
        
    def role_object_grants(self):
        """ - Reads grants from the source account
            - Creates object grants for roles in the target account
            - Future grants not supported yet
        """
        
        sql = """select * from snowflake.account_usage.grants_to_roles;"""
        df_grants  = fetch_data_df(sql, self.source_conn)
        
        supported_object_types = ['WAREHOUSE', 'DATABASE', 'SCHEMA', 'TABLE', 'VIEW']
        df_obj_grants = df_grants[df_grants['GRANTED_ON'].isin(supported_object_types)]
        
        # filter out any snowflake objects
        snowflake_obj_list = ['SNOWFLAKE_SAMPLE_DATA', 'SNOWFLAKE']
        df_obj_grants = df_obj_grants[~df_obj_grants['NAME'].isin(snowflake_obj_list)]
        
        privileges = df_obj_grants['PRIVILEGE'].values.tolist()
        object_types = df_obj_grants['GRANTED_ON'].values.tolist()
        object_names = df_obj_grants['NAME'].values.tolist()
        object_name_schemas = df_obj_grants['TABLE_SCHEMA'].values.tolist()
        object_name_dbs = df_obj_grants['TABLE_CATALOG'].values.tolist()
        grantee_roles = df_obj_grants['GRANTEE_NAME'].values.tolist()

        grants_sql_list = []
        for i in range(0, len(df_obj_grants)):  
            privilege = privileges[i]
            object_type = object_types[i]
            object_name = object_names[i]
            grantee_role = grantee_roles[i]
            object_name_schema = object_name_schemas[i]
            object_name_db = object_name_dbs[i]

            # Some objects need a full name/path
            if object_type == 'TABLE' or object_type == 'VIEW':
                full_object_name = f"{object_name_db}.{object_name_schema}.{object_name}"
            elif object_type == 'SCHEMA':
                full_object_name = f"{object_name_db}.{object_name}"
            else:
                full_object_name = object_name

            # some restrictions on granting ownership
            if privilege == 'OWNERSHIP':   
                sql = [f"""GRANT {privilege} ON {object_type} {full_object_name} TO ROLE  {grantee_role} REVOKE CURRENT GRANTS; """]
            else:
                sql = [f"""GRANT {privilege} ON {object_type} {full_object_name} TO ROLE  {grantee_role}; """]

            grants_sql_list += sql
            
        execute_sql_list(grants_sql_list, self.target_cur, return_sql = self.return_sql, return_errors = True)
        
        
    def copy_account(self):
        """ Function to create all objects. Specific object selection not supported yet """
        
        self.database_objects()
        self.users()
        self.roles()
        self.warehouses()
        self.user_role_grants()
        self.role_role_grants()
        self.role_object_grants()
        
        print("created account objects")
        
        
        
        
    def drop_objects(self, objects = 'all'):
        """ Drops all created objects. Depends on other functions being ran """
        
        if objects == 'all':
            execute_sql_list(self.sql_drop_list, self.target_cur, self.return_sql )
            self.sql_drop_list = []
            
        if objects == 'databases':
            execute_sql_list(self.db_drop_sql_list, self.target_cur, self.return_sql)
            self.db_drop_sql_list = []
            
        if objects == 'users':
            execute_sql_list(self.drop_user_sql_list, self.target_cur, self.return_sql)
            self.drop_user_sql_list = []
            
        
        if objects == 'roles':
            execute_sql_list(self.self.drop_roles_sql_list, self.target_cur, self.return_sql)
            self.drop_roles_sql_list = []
        
        if objects == 'warehouses':
            execute_sql_list(self.drop_wh_list, self.target_cur, self.return_sql)
            self.drop_wh_list = []
            
        

    
    