import pandas as pd
import snowflake.connector 
import configparser



class terraform_transcribe:
    
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        
        self.user = self.config['snowflake']['user']
        self.password = self.config['snowflake']['password']
        self.account = self.config['snowflake']['account']
        
        self.conn = snowflake.connector.connect(
                user = self.user,
                password = self.password,
                account = self.account
            )
        
        self.cur = self.conn.cursor()
      
    
    
    def create_role_resource(self):
        
        sql = 'show roles'
        roles_df = pd.read_sql(sql, self.conn)
        print("total rows: ", )

        tf_roles = []
        for i in range(0, len(roles_df)):
            role = roles_df['name'].values[i]
            comment = roles_df['comment'].values[i]

            # comment is an optional parameter
            if comment == '':
                tf_role = (f"""resource "snowflake_role" "{role}" {{ \n \t name = "{role}" \n }} \n \n""")
            else:
                tf_role = f"""resource "snowflake_role" "{role}" {{ \n \t name = "{role}" \n \t comment = "{comment}" \n }} \n \n"""

            tf_roles.append(tf_role)

        tf_roles_str = ''.join(tf_roles)

        return tf_roles_str

    
    
    def create_user_resource(self):
        
        sql = 'show users'
        users_df = pd.read_sql(sql, self.conn)
        
        print("total users: ", len(users_df))

        tf_users = []
        for i in range(0, len(users_df)):
            name = users_df['name'].values[i]
            login_name = users_df['login_name'].values[i]
            comment = users_df['comment'].values[i]
            disabled = users_df['disabled'].values[i]
            display_name = users_df['display_name'].values[i]
            email = users_df['email'].values[i]
            first_name = users_df['first_name'].values[i]
            last_name = users_df['last_name'].values[i]
            default_warehouse = users_df['default_warehouse'].values[i]
            default_role = users_df['default_role'].values[i]
            must_change_password = users_df['must_change_password'].values[i]

            tf_user = f"""resource "snowflake_user" "{name}" {{ \n \
                          name         = "{name}" \n \
                          login_name   = "{login_name}" \n \
                          comment      = "{comment}" \n \
                          disabled     = {disabled} \n \
                          display_name = "{display_name}" \n \
                          email        = "{email}" \n \
                          first_name   = "{first_name}" \n \
                          last_name    = "{last_name}" \n \

                          default_warehouse = "{default_warehouse}" \n \
                          default_role      = "{default_role}" \n \

                          must_change_password = {must_change_password} \n }} \n \n"""


            tf_users.append(tf_user)

        tf_users_str = ''.join(tf_users)

        return tf_users_str

    
    
    def create_role_grants_resource(self):
        sql = 'show roles'
        roles_df = pd.read_sql(sql, self.conn)
        
        # get grants for all roles (also shows users)
        grants_df = pd.DataFrame()
        for i in range(0, len(roles_df)):
            role = roles_df['name'].values[i]
            sql = f'show grants of role "{role}"'
            print(sql)
            grant_df = pd.read_sql(sql, self.conn)

            grants_df = pd.concat([grant_df, grants_df], ignore_index=True)
        
        
        tf_role_map = []
        for i in range(0, len(grants_df['role'].unique())):
            role = grants_df['role'].unique()[i]
            print(role)

            role_users = grants_df[(grants_df['role'] == role) & \
                              (grants_df['granted_to'] == 'USER')]['grantee_name'].values.tolist()
            role_roles = grants_df[(grants_df['role'] == role) & \
                              (grants_df['granted_to'] == 'ROLE')]['grantee_name'].values.tolist()

            role_users_str = "[ \n" + ", \n".join([f""" \t \t "${{ {unique_user} }}" """ for unique_user in role_users]) + " \n \t ]"
            role_roles_str = "[ \n" + ", \n".join([f""" \t \t "${{ {unique_role} }}" """ for unique_role in role_roles]) + " \n \t ]"

            tf_grants = f"""resource "snowflake_role_grants" "{role}_grants" {{
                    role_name = "${{snowflake.role.role.{role}}}"  \n
                    roles = {role_roles_str} \n
                    users = {role_users_str}  \n }}  \n \n"""

            tf_role_map.append(tf_grants)

        tf_users_str = ''.join(tf_role_map)
        
        return tf_users_str

        
    def close_conn(self):
        self.conn.close()
        return print("closed connection")
    
    def generate_files(self):
        tf_roles_all = self.create_role_resource()
        with open('tf_roles.txt', 'w') as f:
            f.write(tf_roles_all)
        tf_users_all = self.create_user_resource()
        with open('tf_users.txt', 'w') as f:
            f.write(tf_users_all)
        tf_grants_all = self.create_role_grants_resource()
        with open('tf_grants.txt', 'w') as f:
            f.write(tf_grants_all)



if __name__== "__main__":
    # test running from config file called snowflake.config
    tf_conn = terraform_transcribe('snowflake.config')
    tf_conn.generate_files()
