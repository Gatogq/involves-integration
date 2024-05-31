from tests.test2 import fetch_involves_data, extract_json_keys, set_null_values, basic_auth
import pandas as pd
import json
import os
import pyodbc
from datetime import timedelta
from prefect import task, flow


def static_cache_fn(context,parameters):
    return 'static'


@task(name='Conexión a SQL SERVER',retries=2)
def sql_database_connection(server,driver,trusted_connection=True,database=None,user=None,password=None):
    if trusted_connection:
        connection_string = (
            f"""DRIVER={driver};
                 SERVER={server};
                 Trusted_Connection=yes;
                 """
            )
    else:
        connection_string = (
            f"""DRIVER={driver};
                SERVER={server};
                UID={user};
                PWD={password};
                """
            )
    if database:
        connection_string += f"DATABASE={database};"

    return pyodbc.connect(connection_string)


@task(name='Tabla Empleados',cache_key_fn=static_cache_fn,cache_expiration=timedelta(days=1),retries=2)
def fetch_employee_data(auth,environment):
    
    output = 'cached/employees.json'

    if os.path.exists(output):

        with open(output,'r',encoding='utf-8') as j:

            data = json.load(j)
        
    else:

        endpoint = f'/v1/{environment}/employeeenvironment'

        params = {

            'size' : 200
        
        }

        response = fetch_involves_data(auth=auth,endpoint=endpoint,params=params)

        fields = ['id','name','userGroup.name','employeeEnvironmentLeader.name']

        table = [{key : extract_json_keys(d, key) for key in fields} for d in response]

        with open(output,'w',encoding='utf-8') as json_file:

            data = json.dump(table,json_file, indent=3 ,ensure_ascii=False)

    
    return pd.DataFrame(data).map(set_null_values)


@task(name="Tabla Puntos de Venta",cache_key_fn=static_cache_fn,cache_expiration=timedelta(days=1),retries=2)
def fetch_pos_data(auth,environment):

    output = 'cached/pointsofsale.json'

    if os.path.exists(output):

        with open(output,'r',encoding='utf-8') as j:

            data = json.load(j)

    else:

        endpoint = f'/v1/{environment}/pointofsale'

        params = {
        
            'size': 200

        }

        response = fetch_involves_data(auth=auth,endpoint=endpoint,params=params)

        fields = ['id', 'name', 'code', 'region.id', 'chain.id', 'pointOfSaleType.id', 
                'pointOfSaleProfile.id', 'pointOfSaleChannel.id', 'address.zipCode', 'address.latitude', 
                'address.longitude', 'address.city.name', 'address.city.state.name','storeNumber'
                ]
    
        table = [{key : extract_json_keys(d, key) for key in fields} for d in response]

        with open(output,'w',encoding='utf-8') as json_file:

            data = json.dump(table,json_file, indent=3 ,ensure_ascii=False)

    
    return pd.DataFrame(data).map(set_null_values)

@task(name='Actualizar Tabla SQL',log_prints=True)
def insert_df_into_table(connection, table_name, df, delete=False):
    
    with connection.cursor() as cursor:
        try:
            if df.empty:
                print(f'No se encontraron datos que añadir a la tabla: {table_name}.')
                return
            if delete:
                delete_query = f'DELETE FROM {table_name}'
                cursor.execute(delete_query)

            placeholders = ', '.join(['?' for _ in df.columns])
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Prepare a list of tuples containing the parameter values for executemany
            values_list = [tuple(row) for _, row in df.iterrows()]

            # Execute the SQL statement with executemany
            cursor.executemany(insert_query, values_list)

            connection.commit()
            records = len(df)
            print(f'Se actualizó correctamente la tabla: {table_name}. Se escribieron {records} registros.')

        except Exception as e:
            connection.rollback()
            raise BaseException(f"Se obtuvo el siguiente error al intentar actualizar la tabla {table_name}: {e}")


@task(name="Limpiar Cache",log_prints=True)
def clean_cache(folder='cached'):
    try:

        files = os.listdir(folder)
        for file_name in files:
            file_path = os.path.join(folder, file_name)
            os.remove(file_path)
        
        print("Cache cleaned successfully.")

    except Exception as e:
        raise BaseException(f"Error cleaning cache: {e}")


@flow(name='Actualizar BD Involves_DKT')
def main(username,password,env,server,database,driver):

    auth = basic_auth(username,password)

    x = fetch_employee_data(auth=auth,environment=env)
    y = fetch_pos_data(auth=auth,environment=env)
    conn = sql_database_connection(server=server,driver=driver,database=database)
    insert_x = insert_df_into_table(connection=conn,table_name='Employee',df=x, delete=True,wait_for=[x])
    #insert_df_into_table(connection=conn,table_name='PointOfSale',df=y,delete=True,wait_for=[y])
    clean_cache(wait_for=[insert_x])





if __name__ == "__main__":

    username = 'sistemas'
    password = 'sistemas'
    env=1
    server = '172.16.0.7'
    database = 'Involves_DKT'
    driver = '{ODBC Driver 17 for SQL Server}'

    main(username,password,env,server,database,driver)





