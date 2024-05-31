from src.utilities import *
from datetime import timedelta
from prefect import flow, task
from prefect.deployments import Deployment
import numpy as np
import pyodbc


def success_hook(flow, flow_run, state):

    from prefect.blocks.notifications import MicrosoftTeamsWebhook

    teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notifications-webhook")

    completion_time = datetime.now()

    teams_webhook_block.notify(f"""Se ejecutó correctamente el flujo {flow.name}. 
                               flujo concluido con status {state} en {completion_time}.
                               para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run.flow_id}""")
    
def failure_hook(flow, flow_run, state):

    from prefect.blocks.notifications import MicrosoftTeamsWebhook

    teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notifications-webhook")

    completion_time = datetime.now()

    teams_webhook_block.notify(f"""Ocurrió un error al intentar ejecutar el flujo {flow}.
                                flujo concluido con status {state} en {completion_time}.
                                para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run}""")




@task(name='Establecer conexion SQL',log_prints=True)
def set_sql_database_connection(server,driver='{ODBC Driver 17 for SQL Server}',trusted_connection=True,database=None,user=None,password=None):  

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

    try:

        connection = pyodbc.connect(connection_string)
        print(f'''Conexion al servidor realizada con exito.
               Parametros de conexion:  Server: {server}.   Database: {database}.''')
        
        return connection

    except pyodbc.Error as e:

        raise(f'Error al intentar establecer una conexion con el servidor {server}: {e}')



@task(name='Descarga tabla PDV',log_prints=True)
def get_pointofsale_table(auth,env,fields,at_page=None):

    endpoint = f'/v1/{env}/pointofsale'

    params = {
        
        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params,at_page=at_page)

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)


    return df


@task(name='Descarga tabla Usuarios',log_prints=True)
def get_employee_table(auth,env,fields,at_page=None):

    endpoint = f'/v1/{env}/employeeenvironment'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params,at_page=at_page)

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df


@task(name='Descarga tabla Cadenas PDV',log_prints=True)
def get_chain_table(auth,env,at_page=None):

    endpoint = f'/v1/{env}/chain'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params,at_page=at_page)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df


@task(name='Descarga tabla Tipos PDV',log_prints=True)
def get_type_table(auth):

    endpoint = '/v1/pointofsaletype/find'

    params =     {

        'size': 200
        }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,key='itens',params=params)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df

@task(name='Descarga tabla Perfiles PDV',log_prints=True)
def get_profile_table(auth,env):

    endpoint = f'/v1/{env}/pointofsaleprofile/find'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,params=params,paginated=False)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df



@task(name='Descarga tabla Canales PDV',log_prints=True)
def get_channel_table(auth):

    endpoint = f'/v3/pointofsalechannels'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df


@task(name='Descarga tabla Regiones',log_prints=True)
def get_regional_table(auth,env):

    endpoint = f'/v3/environments/{env}/regionals'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params)

    fields = ['id','name','macroregional.id']
    
    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df


@task(name='Descarga Tabla Usuarios/Region',log_prints=True)
def get_employeeregion_table(employee_df,auth,env,exclude=[40,57,92,93]):

    employees = list(filter(lambda x: x not in exclude, employee_df['id'].to_list()))

    dfs = []

    for employee in employees:

        endpoint = f'/v3/environments/{env}/regionals'

        params = {

            'employeeId': employee  
        }

        response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params)

        fields = ['id']

        df = pd.DataFrame(

        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)


        df['employeeId'] = employee

        dfs.append(df)
    
    df_ = pd.concat(dfs,ignore_index=True).map(set_null_values)  

    return df_

@task(name='Descarga tabla Macroregionales',log_prints=True)
def get_macroregional_table(regional_df,auth,env):

    macroregionals = regional_df['id'].drop_duplicates().to_list()

    dfs = []

    for macroregional in macroregionals:


        endpoint = f'/v3/environments/{env}/macroregionals/{macroregional}'

        response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = {

            'id' : response.get('id'),
            'name' : response.get('name')
        }

        dfs.append(data)
    
    df = pd.DataFrame(dfs)

    return df

@task(name='Descarga tabla Ausencias',log_prints=True)
def get_absence_table(auth,env):

    endpoint = f'/v1/{env}/employeeabsence'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','absenceStartDate','absenceEndDate','employeeEnvironmentSuspended.id','reasonNote','absenceNote']


    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df.drop_duplicates()

@task(name='Descarga tabla Productos',log_prints=True)
def get_product_table(auth,env):

    endpoint = f'/v3/environments/{env}/skus'

    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params)

    fields = ['id','name','barCode','productLine.id','brand.id','category.id','supercategory.id']
    
    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df

@task(name='Descarga tabla marca Productos',log_prints=True)
def get_brand_table(auth,env):
    
    endpoint = f'/v3/environments/{env}/brands'
    
    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df


@task(name='Descarga tabla Categoria Producto',log_prints=True)
def get_category_table(product_df,auth,env):

    categories = product_df['category.id'].drop_duplicates().to_list()

    dfs = []

    for category in categories:


        endpoint = f'/v3/environments/{env}/categories/{category}'

        response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = {

            'id' : response.get('id'),
            'name' : response.get('name')
        }

        dfs.append(data)
    
    df = pd.DataFrame(dfs)

    return df

@task(name='Descarga tabla Supercategorias',log_prints=True)
def get_supercategory_table(product_df,auth,env):

    supercategories = product_df['supercategory.id'].drop_duplicates().to_list()

    dfs = []

    for supercategory in supercategories:


        endpoint = f'/v3/environments/{env}/supercategories/{supercategory}'

        response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = {
            'id' : response.get('id'),
            'name' : response.get('name')
        }

        dfs.append(data)
    
    df = pd.DataFrame(dfs)

    return df

@task(name='Descarga tabla Linea Producto',log_prints=True)
def get_productline_table(product_df,auth,env):

    lines = product_df['productLine.id'].drop_duplicates().to_list()

    dfs = []

    for line in lines:


        endpoint = f'/v3/environments/{env}/productlines/{line}'

        response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = {

            'id':response.get('id'),
            'name':response.get('name')

            }
        
        print(data)


        dfs.append(data)
    
    df = pd.DataFrame(dfs)

    return df

@task(name='Descarga tabla Formularios',log_prints=True)
def get_form_table(auth,env):
        
    endpoint = f'/v1/{env}/form/formsActivated'
    
    params = {

        'size': 200
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,params=params,paginated=False)

    fields = ['id','name']

    df = pd.DataFrame(
        [
            {key: extract_json_keys(value, key) for key in fields} 
            for value in response

            ]
            
            ).map(set_null_values)

    return df

@task(name='Descarga tabla campos Formularios',log_prints=True)
def get_formfield_table(form_df,auth,env):

    forms = form_df['id'].drop_duplicates().to_list()

    dfs = []

    for form in forms:


        endpoint = f'/v1/{env}/form/formFields/{form}'

        response = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)

        df = pd.DataFrame(response)

        df['form.id'] = form

        dfs.append(df)
    
    df_ = pd.concat(dfs,ignore_index=True)

    id_col = 'form.id'

    columns = [id_col] + [col for col in df.columns if col != id_col]

    df_ = df_[columns]

    return df_


@task(name='Descarga Encuestas')
def get_surveys(sql_connection,auth,env,form_id=None):

    endpoint = f"/v3/environments/{env}/surveys"

    params = {

        'status': 'ANSWERED',
        'formIds': form_id
    }

    response = involves_paginated_request(auth=auth,endpoint=endpoint,params=params,key=None,paginated=False)

    last_value = last_survey_from_query(sql_connection=sql_connection,form_id=form_id)

    survey_ids = pd.DataFrame(response).query(f'id > {last_value}')['id'].drop_duplicates().to_list()
    
    detail_list = []        
    header_list = []

    for id in survey_ids:
    
        endpoint = f"/v3/environments/{env}/surveys/{id}"

        response_  = involves_paginated_request(auth=auth,endpoint=endpoint,key=None,paginated=False)
 
        fields = ['id','responseDate','pointOfSaleId','ownerId','form.id']

        header = extract_nested_keys(response_,fields)

        ordered_header = {

            'form_id': header['form.id'],
            'survey_id': header['id'],
            'response_date': header['responseDate'],
            'employee_id': header['ownerId'],
            'customer_id': header['pointOfSaleId']
        }   

        header_list.append(ordered_header)

        for answer in response_['answers']:

            survey_id = response_['id']
            item_id = answer['item']['id'] if answer['item'] else None
            topic = answer['item']['topic'] if answer['item'] else None
            question_id = answer['question']['id']
            value = answer['value']

            ordered_detail = {

                'survey_id': survey_id,
                'item_id': item_id,
                'topic': topic,
                'question_id': question_id,
                'value': value

            }

            detail_list.append(ordered_detail)

    header_df = pd.DataFrame(header_list)
    detail_df = pd.DataFrame(detail_list)


    return header_df, detail_df


@task(name='Descarga Visitas',log_prints=True,retries=3)            
def download_visits(username,password,date,env,wait=10,download_folder=None,headless_mode=False,file_name ='informe-gerencial-visitas.xlsx'):
    
    if download_folder is None:
        download_folder = mkdtemp()

    visits = join(download_folder,file_name)
    
    try:
        print("Iniciando proceso de descarga en https://dkt.involves.com/login")
        chrome_options = webdriver.ChromeOptions()
        prefs = {"download.default_directory" : f'{download_folder}'}
        chrome_options.add_experimental_option("prefs",prefs)

        if headless_mode:
            
            #Abrir Chrome sin interfaz gráfica
            chrome_options.add_argument('--headless')
            #Deshabilitar GPU
            chrome_options.add_argument('--disable-gpu')
        
        #abrir el navegador
        driver = webdriver.Chrome(options=chrome_options)      
        driver.get('https://dkt.involves.com/login')
        driver.implicitly_wait(10) #10 segundos máximos de espera, de lo contrario devuelve exception

        # Esperar el inputbox 'username' 
        username_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'username'))
        )
        username_input.clear()
        username_input.send_keys(username)

        # Esperar el inputbox 'password'
        password_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, 'password'))
        )
        password_input.clear()
        password_input.send_keys(password)  

        print("Inicio de Sesión satisfactorio")
        # esperar el submit-button
        
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'inv-btn.submit-button'))
        )
        login_button.click()

        # esperar a que se inicie la sesión
        #driver.implicitly_wait(10)
        #div_element = WebDriverWait(driver, 10).until(
        #    EC.presence_of_element_located((By.CLASS_NAME, 'c-unidade__header-label'))
        #)
        sleep(3)

        #ir al panel de visitas
        if env == 5:
            page_id = 'czEU~2FKSKS0bUKeGe5uBOwQ=='

        if env == 1:
            page_id = 'Mflfx4vR~2FUIfTPLg5S4O8Q=='

        driver.get(f'https://dkt.involves.com/webapp/#!/app/{page_id}/paineldevisitas')
        
        #Esperar el filtro de fecha
        input_element = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'c-input'))
            )
        input_element.clear()
        input_element.send_keys(involves_date(date))
        sleep(2)
        button_element = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.XPATH,'//ap-button[@identifier="searchAggregatorFilter"]')
            ))
        button_element.click()
        sleep(10)
        #esperar el dropdown-toggle-button 'opciones'
        button_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.dropdown-toggle'))
        )

        #click en 'opciones' y luego en 'informe gerencial'
        button_element.click()
        sleep(1)
        driver.implicitly_wait(15)
        li_element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//li[@label="\'report.custom.columns\'"]'))
        ) 
        li_element.click()
    
        #Esperar el modal
        modal_content_element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'modal-content'))
        )
        sleep(1)
        #Elegir celdas ocultas en caso de que existan
        try:
            hidden_column = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.c-button.c-button--success.c-table-options__list-item-btn.u-border-radius-none'))
            )
            hidden_column.click()
        except Exception as e:
            pass
        sleep(2)
        #Cuando el modal se ha cargado, esperar el boton 'confirmar'
        confirm_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//button[contains(., "Confirmar")]'))
        )
        confirm_button.click()

        sleep(wait)

        print("Descarga de visitas satisfactoria")
    except Exception as e:
        print(f"Error en la función download_visits: {e}")
    
    finally:
        driver.quit()
        df = pd.read_excel(visits)
        remove(visits)

        if env == 5:
            df['Fecha de la visita'] = pd.to_datetime(df['Fecha de la visita'],format="%d/%m/%Y")
            df['ID del PDV'] = pd.to_numeric(df['ID del PDV'])
            df['Regional'] = df['Regional'].astype(str)
            df['Tipo de check-in'] = df['Tipo de check-in'].astype(str)
            df['Primer check-in manual'] = pd.to_datetime(df['Primer check-in manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')
            df['Último check-out manual'] = pd.to_datetime(df['Último check-out manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')

            df = df[['Fecha de la visita','ID del PDV','Regional','Tipo de check-in',
                     'Primer check-in manual','Último check-out manual']]
        
        if env == 1:
            df['Fecha de la visita'] = pd.to_datetime(df['Fecha de la visita'],format="%d/%m/%Y")
            df['ID del PDV'] = pd.to_numeric(df['ID del PDV'])
            df['Regional'] = df['Regional'].astype(str)
            df['Tipo de check-in'] = df['Tipo de check-in'].astype(str)
            df['Primer check-in manual'] = pd.to_datetime(df['Primer check-in manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')
            df['Último check-out manual'] = pd.to_datetime(df['Último check-out manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')

            df = df[['Fecha de la visita','ID del PDV','Empleado','Tipo de check-in','Primer check-in manual',
                     'Último check-out manual','Total de encuestas respondidas','Motivo para no realizar la visita']]


    return df


@task(name='Actualizar Tabla SQL',log_prints=True)
def insert_df_into_table(connection, table_name, df, delete=False):

    df = df.replace({np.NaN: None})
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

            values_list = [tuple(row) for _, row in df.iterrows()]

            cursor.executemany(insert_query, values_list)

            connection.commit()

            records = len(df)

            print(f'Se actualizó correctamente la tabla: {table_name}. Se escribieron {records} registros.')

        except Exception as e:

            connection.rollback()
            print(f"Se obtuvo el siguiente error al intentar actualizar la tabla {table_name}: {e}")


@flow(
        name='actualizacion_db_involves_dkt',
        description='actualiza las tablas en la base de datos involves_dkt',
        log_prints=True
        )
def update_involves_dkt(env=1):

    from dotenv import load_dotenv
    from os import getenv

    load_dotenv('config/.env')
    username = getenv('USER')
    password =getenv('PASSWORD')
    server = getenv('SERVER')

    auth = basic_auth(username,password)

    connection = set_sql_database_connection(server=server,database='Involves_DKT')

    get_employee_table(

        auth=auth,
        env=env,
        fields=[
                'id',
                'name',
                'userGroup.name',
                'employeeEnvironmentLeader.name'
                ]

             )
             
    get_regional_table(auth=auth,env=env)

@flow(
        name='actualizacion_db_involves_clinical',
        description='actualiza las tablas en la base de datos de involves clinical',
        on_completion=[success_hook],
        on_failure=[failure_hook],
        on_crashed=[failure_hook]
        )
def update_involves_clinical(env=5):
        
        from prefect.blocks.system import JSON
        

        env_vars = JSON.load("involves-workpool-environment-variables").value

        username = env_vars['USER']
        password = env_vars['PASSWORD']
        server = env_vars['SERVER']

        auth = basic_auth(username,password)

        connection = set_sql_database_connection(server=server,database='Involves')
        
        types = get_type_table.submit(auth=auth)
        channels = get_channel_table.submit(auth=auth)
        employees = get_employee_table.submit(
            auth=auth,
            env=env,
            fields=[
                    'id',
                    'name',
                    'nationalIdCard2',
                    'userGroup.name',
                    'employeeEnvironmentLeader.id',
                    'email',
                    'enabled',
                    'fieldTeam',
                    'Phone',
                    'Hierarchy'
                    ]

                )
        pos = get_pointofsale_table.submit(
            auth=auth,
            env=env,
            fields=[
                'pointOfSaleBaseId',
                'name',
                'code',
                'phone',
                'enabled',
                'region.name',
                'pointOfSaleType.Id',
                'pointOfSaleProfile.Id',
                'pointOfSaleChannel.Id',
                'address.zipcode',
                'address.city.name',
                'address.city.state.name',
                'address.latitude',
                'address.longitude'
            ]
        )

        visits = download_visits.submit(username=username,
                                 password=password,
                                 date=datetime(2023,11,1),env=env,
                                headless_mode=True)

        with connection as conn:
            
            insert_df_into_table(connection=conn,table_name='POSTypes',df=types,delete=True,wait_for=[types])
            insert_df_into_table(connection=conn,table_name='POSChannels',df=channels,delete=True,wait_for=[channels])
            insert_df_into_table(connection=conn,table_name='Employee',df=employees,delete=True,wait_for=[employees])
            insert_df_into_table(connection=conn,table_name='PointOfSale',df=pos,delete=True,wait_for=[pos])
            insert_df_into_table(connection=conn,table_name='Visit',df=visits,delete=True,wait_for=[visits])

        

