from src.involves_client import InvolvesAPIClient
from src.sql_engine import SQLServerEngine
from src.utilities import set_null_values
from datetime import datetime
import pandas as pd
from prefect import task, flow
from prefect.blocks.system import JSON
from prefect.artifacts import create_markdown_artifact


@task(name='actualizar tabla SQL',log_prints=True)
def update_table(SQLSession,dfs,table,primary_key):

     if 'insert' in dfs:

          df = dfs['insert']

          if not df.empty:

               SQLSession.bulk_insert_from_df(table_name=table,df=df)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros nuevos')
    
          else:
               print(f'No se encontraron registros modificados para actualizar en la tabla {table}')
     
     if 'update' in dfs:

          df = dfs['update']
     
          if not df.empty:
               
               SQLSession.update_records_from_df(table_name=table,df=df,primary_key=primary_key)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros actualizados')

          else:
               print(f'No se encontraron registros nuevos para añadir a la tabla {table}')

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

     last_update_timestamp = SQLSession.get_last_update_timestamp(table=table,time_column='updated_at')

     rows = Client.get_pointsofsale(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = pd.DataFrame(rows).assign(updated_at=timestamp).map(set_null_values)

            ids = set(SQLSession.select_values(table=table,column='id'))

            df_to_insert = df[~df['id'].isin(ids)]

            df_to_update = df[df['id'].isin(ids)]

    
     else:

            df_to_insert = pd.DataFrame({})
            df_to_update = pd.DataFrame({})
            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          'insert':df_to_insert,
          'update':df_to_update
     } 
     


@task(name='descarga empleados',log_prints=True)
def get_employee_data(Client,SQLSession,fields,table):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update_timestamp(table=table,time_column='updated_at')

     rows = Client.get_employees(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = pd.DataFrame(rows).assign(updated_at=timestamp).map(set_null_values)

            ids = set(SQLSession.select_values(table=table,column='id'))

            df_to_insert = df[~df['id'].isin(ids)]

            df_to_update = df[df['id'].isin(ids)]
            
     else:

            df_to_insert = pd.DataFrame({})
            df_to_update = pd.DataFrame({})
            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          'insert': df_to_insert,
          'update':df_to_update
          }

@flow(log_prints=True)
def update_involves_clinical_db(block='involves-clinical-env-vars'):
     
     vars = JSON.load(block).value

     environment = vars['ENVIRONMENT']
     domain = vars['DOMAIN']
     username = vars['USERNAME']
     password = vars['PASSWORD']
     server = vars['SERVER']
     database = vars['DATABASE']

     Client = InvolvesAPIClient(environment,domain,username,password)
     SQLSession = SQLServerEngine(engine_type='sqlite',database='example.db')

     pos = get_pointofsale_data.submit(Client=Client,SQLSession=SQLSession,
                                           fields=['id','name','code','enabled','region_name',
                                         'chain_name','pointOfSaleType','pointOfSaleProfile','pointOfSaleChannel_name',
                                         'address_zipCode','address_latitude','address_longitude','deleted'],table='PointOfSale'
                                         )
     employees = get_employee_data.submit(Client=Client,SQLSession=SQLSession,
                                        fields=['id','name'],table='Employee'
                                         )
     
     update_table.map(SQLSession=SQLSession,dfs=[pos,employees],table=['PointOfSale','Employee'],primary_key='id')


     
     #insert_records(SQLSession=SQLSession,df=employees_to_insert,table='Employee')
     #update_records(SQLSession=SQLSession,df=employees_to_update,table='Employee',primary_key='id')
     
     
     
     

     
     


if __name__ == "__main__": 

     update_involves_clinical_db()    

    # #Initialize InvolvesAPIClient object for further interaction with Involves Stage API
    # Client = InvolvesAPIClient(environment=5,domain='dkt',username='sistemas',password='sistemas')

    # #Initialize SQLSession object for further database operations within encapsulated Sessions
    # SQLSession = SQLServerEngine(engine_type='sqlite',database='example.db')

    # #timestamp in milliseconds since unix epoch time
    # timestamp = int(datetime.now().timestamp()*1000)

    # #get the timestamp corresponding to the last database update
    # last_update_timestamp = SQLSession.get_last_update_timestamp(table='PointOfSale',time_column='updated_at') 


    # #api call to get points of sale that were created or modified since last database update
    # rows = Client.get_pointsofsale(select=['id','name','code','enabled','region_name',
    #                                     'chain_name','pointOfSaleType','pointOfSaleProfile','pointOfSaleChannel_name',
    #                                     'address_zipCode','address_latitude','address_longitude','deleted'
    #                                     ],updatedAtMillis=last_update_timestamp)

    # if rows:

    # #algorithm that returns two dataframes: df_to_insert contains new records, while df_to_update contains
    # #modified records that already exist on the database.

    #     df = pd.DataFrame(rows).assign(updated_at=timestamp).map(set_null_values)

    #     ids = set(SQLSession.select_values(table='PointofSale',column='id'))

    #     df_to_insert = df[~df['id'].isin(ids)]

    #     df_to_update = df[df['id'].isin(ids)]

    #     #bulk insert operation with df_to_insert
    #     if not df_to_insert.empty:

    #             SQLSession.bulk_insert_from_df(table_name='PointOfSale',df=df_to_insert)

    #     else:
    #         print(f'No se encontraron registros nuevos para añadir a la tabla PointOfSale')

    # #update operation with df_to_update
    #     if not df_to_update.empty:
            
    #         SQLSession.update_records_from_df(table_name='PointOfSale',df=df_to_update,primary_key='id')

    #     else:
    #         print(f'No se encontraron registros modificados para actualizar en la tabla PointOfSale')
    # else:
    #     print('No se añadieron o modificaron registros desde la última actualización')

