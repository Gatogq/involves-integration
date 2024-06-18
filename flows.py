from src.involves_client import InvolvesAPIClient
from src.sql_engine import SQLServerEngine
from src.utilities import set_null_values,download_visits
from datetime import datetime
from pandas import DataFrame
from prefect import task, flow
from prefect.blocks.system import JSON
from prefect.artifacts import create_markdown_artifact
from numpy import NaN

def success_hook(flow, flow_run, state):

    from prefect.blocks.notifications import MicrosoftTeamsWebhook

    teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notifications-webhook") 

    completion_time = datetime.now()

    teams_webhook_block.notify(f"""Se ejecutó correctamente el flujo {flow.name}. 
                               flujo concluido con status {state} en {completion_time}.
                               para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run.__dict__}""")
    
    
def failure_hook(flow, flow_run, state):

    from prefect.blocks.notifications import MicrosoftTeamsWebhook

    teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notifications-webhook")

    completion_time = datetime.now()

    teams_webhook_block.notify(f"""Ocurrió un error al intentar ejecutar el flujo {flow.name}.
                                flujo concluido con status {state} en {completion_time}.
                                para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run.__dict__}""")


@task(name='actualizar tabla SQL',log_prints=True)
def update_table(SQLSession,dfs,table,primary_key):

     if 'insert' in dfs:

          df = dfs['insert']

          if not df.empty:

               SQLSession.bulk_insert_from_df(table,df)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros nuevos')
    
          else:
               print(f'No se encontraron registros nuevos para actualizar en la tabla {table}')

     if 'update' in dfs:

          df = dfs['update']
     
          if not df.empty:
               
               SQLSession.update_records_from_df(table,df,primary_key)
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


@task(name='descarga puntos de venta',log_prints=True,retries=2)
def get_pointofsale_data(Client,SQLSession,fields,table):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update(table,time_column='updated_at')

     rows = Client.get_pointsofsale(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = DataFrame(rows).assign(updated_at=timestamp).map(set_null_values).replace({NaN:None})

            ids = set(SQLSession.select_values(table,columns=['id']))

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
     
@task(name='descarga empleados',log_prints=True,retries=2)
def get_employee_data(Client,SQLSession,fields,table):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update(table,time_column='updated_at')

     rows = Client.get_employees(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = DataFrame(rows).assign(updated_at=timestamp).map(set_null_values).replace({NaN:None})

            ids = set(SQLSession.select_values(table,columns=['id']))

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


def get_visit_retry_handler(task,task_run,state):

     try: 
          state.result()

     except FileNotFoundError:

          return True

     except:

          return False

     
@task(name='Descarga visitas',log_prints=True,retries=3,retry_condition_fn=get_visit_retry_handler)
def get_visit_data(SQLSession,username,password,environment,domain,fields,table):

     date = SQLSession.get_last_visit_date(table,column='visit_date')

     df = download_visits(username,password,date,environment,domain)

     df.columns = fields

     ids = set(SQLSession.select_values(table,columns=['visit_date','customer_id']))

     df_to_insert = df[~df.apply(lambda row: (row['visit_date'],row['customer_id']) in ids, axis=1)].replace({NaN:None})

     return {

          'insert' : df_to_insert
     }



@flow(name='flujo involves (clinical)',
      description='''este flujo actualiza la base de datos de Involves de Clinical.
      se actualizan registros modificados y se insertan registros nuevos
      en las tablas PointOfSale, Employee, Visit, Channel, Region y Chain''',
      log_prints=True,
      on_completion=[success_hook],
      on_crashed=[failure_hook],
      on_failure=[failure_hook]
      )

def update_involves_cl(environment,domain,username,password,engine_type,database,server):

     Client = InvolvesAPIClient(environment,domain,username,password)
     
     SQLSession = SQLServerEngine(engine_type,server,database)

     pos = get_pointofsale_data.submit(Client=Client,SQLSession=SQLSession,
                                           fields=['id','pointOfSaleBaseId','name','code','enabled','region_id',
                                         'chain_id','pointOfSaleChannel_id',
                                         'address_zipCode','address_city_name','address_city_state_name','address_latitude','address_longitude','deleted'],table='PointOfSale2'
                                         )
     employees = get_employee_data.submit(Client=Client,SQLSession=SQLSession,
                                        fields=['id','name','nationalIdCard2','userGroup_name','email','enabled','fieldTeam'],table='Employee2'
                                         )
     visits = get_visit_data.submit(SQLSession,username,password,environment,domain,fields=['visit_date','customer_id','employee_name',
                                                                   'visit_status','check_in','check_out'],table='Visit2')
     
     update_table.map(SQLSession,dfs=[pos,employees,visits],table=['PointOfSale2','Employee2','Visit2'], primary_key='id')



if __name__ == "__main__": 

     env_vars = JSON.load('involves-clinical-env-vars').value

     environment = env_vars['ENVIRONMENT'],
     domain = env_vars['DOMAIN'],
     username = env_vars['USERNAME'],
     password = env_vars['PASSWORD'],
     engine_type = env_vars['ENGINE']
     database = env_vars['DATABASE']
     server = env_vars['SERVER']

     update_involves_cl(environment=environment,
                        domain=domain,username=username,
                        password=password,engine_type=engine_type,
                        database=database,server=server)