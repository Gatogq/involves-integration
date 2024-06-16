from src.involves_client import InvolvesAPIClient
from src.sql_engine import SQLServerEngine
from src.utilities import set_null_values,download_visits
from datetime import datetime
from pandas import DataFrame
from prefect import task, flow
from prefect.blocks.system import JSON
from prefect.artifacts import create_markdown_artifact
from numpy import NaN


@task(name='actualizar tabla SQL',log_prints=True)
def update_table(SQLSession,dfs,table,primary_key):

     if 'insert' in dfs:

          df = dfs['insert']

          if not df.empty:

               SQLSession.bulk_insert_from_df(table_name=table,df=df)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros nuevos')
    
          else:
               print(f'No se encontraron registros nuevos para actualizar en la tabla {table}')

     if 'update' in dfs:

          df = dfs['update']
     
          if not df.empty:
               
               SQLSession.update_records_from_df(table_name=table,df=df,primary_key=primary_key)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros actualizados')

          else:
               print(f'No se encontraron registros modificados para añadir a la tabla {table}')

     if 'delete' in dfs:
           
           df = dfs['delete']
           
           if not df.empty:
                #---
                create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros eliminados')
          
           else:
               print(f'No se encontraron registros eliminados en la tabla {table}')

@task(name='descarga puntos de venta',log_prints=True)
def get_pointofsale_data(Client,SQLSession,fields,table):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update(table=table,time_column='updated_at')

     rows = Client.get_pointsofsale(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = DataFrame(rows).assign(updated_at=timestamp).map(set_null_values)

            ids = set(SQLSession.select_values(table=table,columns=['id']))

            df_to_insert = df[~df['id'].isin(ids)]

            df_to_update = df[df['id'].isin(ids)]

    
     else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})
            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          'insert':df_to_insert,
          'update':df_to_update
     } 
     
@task(name='descarga empleados',log_prints=True)
def get_employee_data(Client,SQLSession,fields,table):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update(table=table,time_column='updated_at')

     rows = Client.get_employees(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = DataFrame(rows).assign(updated_at=timestamp).map(set_null_values)

            ids = set(SQLSession.select_values(table=table,columns=['id']))

            df_to_insert = df[~df['id'].isin(ids)]

            df_to_update = df[df['id'].isin(ids)]
            
     else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})
            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          'insert': df_to_insert,
          'update':df_to_update
          }

@task(name='Descarga visitas',log_prints=True)
def get_visit_data(SQLSession,username,password,environment,fields,table):

     date = SQLSession.get_last_visit_date(table=table,column='visit_date')

     df = download_visits(username=username,password=password,date=date,env=environment,headless_mode=False)

     df.columns = fields

     ids = set(SQLSession.select_values(table=table,columns=['visit_date','customer_id']))

     df_to_insert = df[~df.apply(lambda row: (row['visit_date'],row['customer_id']) in ids, axis=1)]

     df_to_insert = df_to_insert.replace({NaN: None})

     return {
          'insert' : df_to_insert
     }


@flow(log_prints=True)
def update_involves_clinical_db(block='involves-clinical-env-vars'):
     
     env_vars = JSON.load(block).value

     Client = InvolvesAPIClient(environment=env_vars.get('ENVIRONMENT'),
                                domain=env_vars.get('DOMAIN'),
                                username=env_vars.get('USERNAME'),
                                password=env_vars.get('PASSWORD')
                                )
     
     SQLSession = SQLServerEngine(server=vars.get('SERVER'),database=vars.get('DATABASE'))

     pos = get_pointofsale_data.submit(Client=Client,SQLSession=SQLSession,
                                           fields=['id','pointOfSaleBaseId','name','code','enabled','region_name',
                                         'chain_name','pointOfSaleType','pointOfSaleProfile','pointOfSaleChannel_name',
                                         'address_zipCode','address_latitude','address_longitude','deleted'],table='PointOfSale'
                                         )
     employees = get_employee_data.submit(Client=Client,SQLSession=SQLSession,
                                        fields=['id','name'],table='Employee'
                                         )
     visits = get_visit_data.submit(SQLSession=SQLSession,
                             username=env_vars.get('USERNAME'),password=env_vars.get('PASSWORD'),environment=env_vars.get('ENVIRONMENT'),
                             fields=['visit_date','customer_id','employee_name','visit_status','check_in','check_out'],table='Visit')
     
     update_table.map(SQLSession=SQLSession,dfs=[pos,employees,visits],table=['PointOfSale','Employee','Visit'], primary_key='id')


if __name__ == "__main__": 

     update_involves_clinical_db()