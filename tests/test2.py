from time import sleep
from functools import reduce
from pandas import read_excel, to_datetime, to_numeric, isna
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from os import remove
from os.path import join
from pyodbc import connect
from base64 import b64encode
import requests
from tempfile import mkdtemp
import numpy as np
import pandas as pd
from configparser import ConfigParser
import os
from src.involves_functions import insert_df_into_table, sql_database_connection

def basic_auth(username,password):
    
    credentials = f'{username}:{password}'
    encoded_credentials = b64encode(credentials.encode()).decode()
    
    return f'Basic {encoded_credentials}'

def fetch_involves_data(auth,endpoint,key='items',params=None,at_page=None,paginated=True):

    url = 'https://dkt.involves.com/webservices/api'


    headers = {
        'Authorization': auth,
        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
        }
    
    rows = []
    page = 1
    totalPages = None

    if paginated:

        while totalPages is None or page < totalPages + 1:
            try:
                response = requests.get(f'{url}{endpoint}?page={page}',headers=headers,params=params)
                response.raise_for_status()
                if key is not None:

                    items = response.json().get(key,[])

                else:
                    
                    items = response.json()


                rows.extend(items)

                totalPages = response.json().get('totalPages')

                if at_page is not None:

                    totalPages = at_page

                page += 1
                print(items)

            except Exception as e:
            
                print(f"No se encontró información en: {endpoint} : {e}")
                break
    else:
        response = requests.get(f'{url}{endpoint}',headers=headers,params=params)
        rows = response.json()

    return rows

def extract_json_keys(d, keys):
    try:
        return reduce(lambda d, k: d.get(k, {}), keys.split('.'), d)
    
    except (AttributeError, TypeError):
        return None


def extract_nested_keys(data, keys):
    result = {}
    for key in keys:
        nested_keys = key.split('.')  # Split nested keys by dot notation
        temp = data
        for nested_key in nested_keys:
            if nested_key in temp:
                temp = temp[nested_key]
            else:
                temp = None
                break
        result[key] = temp
    return result


def get_pointofsale(auth,environment_id,at_page=None):

    endpoint = f'/v1/{environment_id}/pointofsale'

    params = {
        
        'size': 200

    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params,at_page=at_page)

    x = pd.DataFrame(dict)
    x.to_excel('test_pos.xlsx')

    fields = ['id', 'name', 'code', 'region.id', 'chain.id', 'pointOfSaleType.id', 
              'pointOfSaleProfile.id', 'pointOfSaleChannel.id', 'address.zipCode', 'address.latitude', 
              'address.longitude', 'address.city.name', 'address.city.state.name','storeNumber']

    # Define keys to extract from customFields
    custom_fields_keys = ['campo1', 'campo2', '1-En ruta']

    # Initialize an empty list to store extracted data
    extracted_data = []

    # Iterate over each x|item in the response data
    for item in dict:
        # Extract values for the defined fields
        extracted_values = {key: extract_json_keys(item, key) for key in fields}

        # Extract values from customFields
        custom_fields = item.get('customFields', [])

        # Extract values for the defined keys from each customField element
        for field_key in custom_fields_keys:
            field_value = next((cf['value'] for cf in custom_fields if cf['name'] == field_key), None)
            extracted_values[field_key] = field_value

        # Append extracted values to the list
        extracted_data.append(extracted_values)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(extracted_data)

    df = pd.concat([df.drop(columns='storeNumber'), df['storeNumber']], axis=1)

    # Rename columns
    df.columns = ['store_id', 'store_name', 'code', 'regional_id', 'chain_id', 'type_id', 'profile_id', 
                  'channel_id', 'zip_code', 'latitude', 'longitude', 'city', 'state'] + custom_fields_keys + ['store_number']
    
    
    df = df.map(replace_dict_with_none)

    return df


def get_employee(auth,environment_id,at_page=None):

    endpoint = f'/v1/{environment_id}/employeeenvironment'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params,at_page=at_page)

    fields = ['id','name','userGroup.name','employeeEnvironmentLeader.name']

    data = pd.DataFrame([{key: extract_json_keys(d, key) for key in fields} for d in dict])

    sql_fields = ['employee_id','employee_name','user_group','leader_name']

    data.columns = sql_fields

    data = data.map(set_null_values)

    return data


def get_chain(auth,environment_id,at_page=None):

    endpoint = f'/v1/{environment_id}/chain'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params,at_page=at_page)

    fields = ['id','name']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['chain_id','chain_name']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data

def get_type(auth,at_page=1):

    endpoint = '/v1/pointofsaletype/find'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='itens',params=params,at_page=at_page)

    fields = ['id','name']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['type_id','type_name']

    data.columns = sql_fields
    data = data.map(replace_dict_with_none)

    return data


def get_profile(auth,environment_id,at_page=1):

    endpoint = f'/v1/{environment_id}/pointofsaleprofile/find'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,params=params,at_page=at_page)

    fields = ['id','name']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['profile_id','profile_name']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data



def get_channel(auth):

    endpoint = f'/v3/pointofsalechannels'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','name']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['channel_id','channel_name']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data


def get_regional(auth,environment_id):

    endpoint = f'/v3/environments/{environment_id}/regionals'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','name','macroregional.id']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['regional_id','regional','macroregional_id']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data


def get_employee_region(employee_df,auth,environment_id,exclude=[40,57,92,93]):

    employees = list(filter(lambda x: x not in exclude, employee_df['employee_id'].to_list()))

    dfs = []

    for employee in employees:

        endpoint = f'/v3/environments/{environment_id}/regionals'

        params = {

            'employeeId': employee  
        }

        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

        fields = ['id']

        data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

        sql_fields = ['regional_id']

        data.columns = sql_fields

        data['employee_id'] = employee

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    df = df.map(replace_dict_with_none)
    

    return df

def get_macroregional(regional_df,auth,environment_id):

    macroregionals = regional_df['macroregional_id'].drop_duplicates().to_list()

    dfs = []

    for macroregional in macroregionals:


        endpoint = f'/v3/environments/{environment_id}/macroregionals/{macroregional}'

        print(endpoint)
        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

        print(dict)

        fields = ['id','name']

        data = pd.DataFrame(dict,index=[0])

        sql_fields = ['macroregional_id','macroregional']

        data.columns = sql_fields

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    return df

def get_absence(auth,environment_id):

    endpoint = f'/v1/{environment_id}/employeeabsence'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','absenceStartDate','absenceEndDate','employeeEnvironmentSuspended.id','reasonNote','absenceNote']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['absence_id','start_date','end_date','employee_id','reason','note']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data.drop_duplicates()

def get_product(auth,environment_id):

    endpoint = f'/v3/environments/{environment_id}/skus'

    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','name','barCode','productLine.id','brand.id','category.id','supercategory.id']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['product_id','product_name','bar_code','productline_id','brand_id','category_id','supercategory_id']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    return data

def get_brand(auth,environment_id):
    
    endpoint = f'/v3/environments/{environment_id}/brands'
    
    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key='items',params=params)

    fields = ['id','name']

    data = pd.DataFrame([{key: get_nested_value(d, key) for key in fields} for d in dict])

    sql_fields = ['brand_id','brand_name']

    data.columns = sql_fields

    data = data.map(replace_dict_with_none)

    print(data)

    return data

def get_category(product_df,auth,environment_id):

    categories = product_df['category_id'].drop_duplicates().to_list()

    dfs = []

    for category in categories:


        endpoint = f'/v3/environments/{environment_id}/categories/{category}'

        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = pd.DataFrame(dict,index=[0])

        sql_fields = ['category_id','category_name','']

        data.columns = sql_fields

        data = data[['category_id','category_name']]

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    df = df.map(replace_dict_with_none)

    return df

def get_supercategory(product_df,auth,environment_id):

    supercategories = product_df['supercategory_id'].drop_duplicates().to_list()

    dfs = []

    for supercategory in supercategories:


        endpoint = f'/v3/environments/{environment_id}/supercategories/{supercategory}'

        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

        data = pd.DataFrame(dict,index=[0])

        sql_fields = ['supercategory_id','supercategory_name']

        data.columns = sql_fields

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    return df

def get_productline(product_df,auth,environment_id):

    lines = product_df['productline_id'].drop_duplicates().to_list()

    dfs = []

    for line in lines:


        endpoint = f'/v3/environments/{environment_id}/productlines/{line}'

        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

        print(dict)

        dict2 = {
            'id':dict.get('id'),
            'name':dict.get('name')
        }

        data = pd.DataFrame(dict2, index=[0])

        sql_fields = ['productline_id','productline']

        data.columns = sql_fields

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    return df

def get_form(auth,environment_id):
        
    endpoint = f'/v1/{environment_id}/form/formsActivated'
    
    params = {

        'size': 200
    }

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,params=params,paginated=False)

    fields = ['id','name']

    data = pd.DataFrame(dict)

    sql_fields = ['form_id','form_title']

    data.columns = sql_fields

    return data

def get_form_fields(form_df,auth,environment_id):

    forms = form_df['form_id'].drop_duplicates().to_list()

    dfs = []

    for form in forms:


        endpoint = f'/v1/{environment_id}/form/formFields/{form}'

        dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)
        
        data = pd.DataFrame(dict)

        print(data)

        sql_fields = ['question_id','question_label','form_id']

        data['form_id'] = form

        data.columns = sql_fields

        dfs.append(data)
    
    df = pd.concat(dfs,ignore_index=True)

    last_column = df.columns[-1]
    df.insert(0, last_column, df.pop(last_column))

    return df


def list_from_file(file_path,column_index):

    values = list()

    with open(file_path, 'r') as file:

        for line in file:

            value = line.strip().split(',')[column_index]


            try:
                value = int(value)

            except (ValueError, IndexError):

                continue

            values.append(value)

    return values




def encuestas_fragua(auth,environment_id):

     #get all answered surveys
    endpoint = f"/v3/environments/{environment_id}/surveys?status=ANSWERED"

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

    survey_ids = pd.DataFrame(dict).query('id > 1820446').query("label == 'Renta Guadalajara'")['id'].drop_duplicates().to_list()
    


    detail_list = []        
    header_list = []

    for id in survey_ids:
    
        endpoint = f"/v3/environments/{environment_id}/surveys/{id}"

        fetched  = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)
 
        fields = ['id','responseDate','pointOfSaleId','ownerId','form.id']

        header = extract_nested_keys(fetched,fields)

        ordered_header = {
            'form_id': header['form.id'],
            'survey_id': header['id'],
            'response_date': header['responseDate'],
            'employee_id': header['ownerId'],
            'customer_id': header['pointOfSaleId']
        }   
        print(ordered_header)

        header_list.append(ordered_header)


        for answer in fetched['answers']:
            survey_id = fetched['id']
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

            print(ordered_detail)
            detail_list.append(ordered_detail)

    header_f = pd.DataFrame(header_list)
    detail_f = pd.DataFrame(detail_list)


    return header_f, detail_f


            #with open('survey_detail', 'a') as file:
                #file.write(','.join(map(str, [survey_id, item_id, topic, question_id, value])) + '\n')



        #with open('survey_header', 'a') as file:
            #file.write(','.join(map(str, ordered_header.values())) + '\n')
        
 


        #if i == 60:
        #    t = 60
        #    print(f'waiting {t} seconds for requesting again...')
        #    sleep(t)
        #    i=0


def involves_date(date):
    day = str(date.day)
    month = str(date.month)
    year = str(date.year)[-2:]
    formatted_date = f"{day}/{month}/{year}"
    return str(formatted_date)

            
def download_visits(username,password,date,download_folder=None,headless_mode=False,file_name ='informe-gerencial-visitas.xlsx'):
    
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
        sleep(5)

        #ir al panel de visitas
        driver.get('https://dkt.involves.com/webapp/#!/app/Mflfx4vR~2FUIfTPLg5S4O8Q==/paineldevisitas')
        
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
        sleep(20)
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

        sleep(65)
        print("Descarga de visitas satisfactoria")
    except Exception as e:
        print(f"Error en la función download_visits: {e}")
    
    finally:
        driver.quit()
        df = read_excel(visits)
        remove(visits)

    return df

def convert_to_none(column):
    if isna(column) or column == '':
        return None
    return column

def format_visits_table(df,date_format="%d/%m/%Y"):
    
    df['Fecha de la visita'] = to_datetime(df['Fecha de la visita'],format=date_format)
    df['ID del PDV'] = to_numeric(df['ID del PDV'])
    df['Regional'] = df['Regional'].astype(str)
    df['Tipo de check-in'] = df['Tipo de check-in'].astype(str)
    df['Primer check-in manual'] = to_datetime(df['Primer check-in manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')
    df['Último check-out manual'] = to_datetime(df['Último check-out manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')

    return df[['Fecha de la visita','ID del PDV','Empleado','Tipo de check-in','Primer check-in manual','Último check-out manual','Total de encuestas respondidas','Motivo para no realizar la visita']]


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

    return connect(connection_string)


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

            # Prepare a list of tuples containing the parameter values for executemany
            values_list = [tuple(row) for _, row in df.iterrows()]

            # Execute the SQL statement with executemany
            cursor.executemany(insert_query, values_list)

            connection.commit()
            records = len(df)
            print(f'Se actualizó correctamente la tabla: {table_name}. Se escribieron {records} registros.')

        except Exception as e:
            connection.rollback()
            print(f"Se obtuvo el siguiente error al intentar actualizar la tabla {table_name}: {e}")

def set_null_values(value):
    if isinstance(value, dict) or value == '':
        return None
    return value

def encuestas_ventas_demo(auth,environment_id):

     #get all answered surveys
    endpoint = f"/v3/environments/{environment_id}/surveys?status=ANSWERED&formIds=16"

    dict = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)

    survey_ids = pd.DataFrame(dict).query('id > 1820018').query("label == 'Respondida vía Informar de la App'")['id'].drop_duplicates().to_list()

    detail_list = []        
    header_list = []

    for id in survey_ids:
    
        endpoint = f"/v3/environments/{environment_id}/surveys/{id}"

        fetched  = fetch_involves_data(auth=auth,endpoint=endpoint,key=None,paginated=False)
 
        fields = ['id','responseDate','pointOfSaleId','ownerId','form.id']

        header = extract_nested_keys(fetched,fields)

        ordered_header = {
            'form_id': header['form.id'],
            'survey_id': header['id'],
            'response_date': header['responseDate'],
            'employee_id': header['ownerId'],
            'customer_id': header['pointOfSaleId']
        }   
        print(ordered_header)

        header_list.append(ordered_header)


        for answer in fetched['answers']:
            survey_id = fetched['id']
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

            print(ordered_detail)
            detail_list.append(ordered_detail)

    header_f = pd.DataFrame(header_list)
    detail_f = pd.DataFrame(detail_list)


    return header_f, detail_f


if __name__ == '__main__':

    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.ini')
    config.read(config_file)

    # Credentials
    username = config.get('Credentials', 'Username')
    password = config.get('Credentials', 'Password')
    # SQL Server configuration
    server = config.get('SQL', 'Server')
    database = 'Involves_DKT'#config.get('SQL', 'Database')
    driver = config.get('SQL', 'Driver')


    auth = basic_auth(username=username,password=password)
    environment_id = 1
    sql_instance = sql_database_connection(server=server,driver=driver,database=database)

    #employees = get_employee(environment_id=environment_id,auth=auth)
    #pos = get_pointofsale(environment_id=1,auth=auth)
    #employee_regions = get_employee_region(employee_df=employees,auth=auth,environment_id=environment_id)
    #regionals = get_regional(environment_id=environment_id,auth=auth)
    #macroregionals = get_macroregional(regional_df=regionals,auth=auth,environment_id=environment_id) 
    #absences = get_absence(auth=auth,environment_id=environment_id)
    #forms = get_form(auth=auth,environment_id=environment_id)
    #form_fields = get_form_fields(form_df =forms,auth=auth,environment_id=environment_id)
    #surveys = encuestas_ventas_demo(auth=auth,environment_id=environment_id)
    #visits = format_visits_table(download_visits(username=username,password=password,date=datetime(2023,1,1)))
    #status_day = format_status_day_table(download_status_day('sistemas','sistemas',date=datetime(2024,3,1)))

    with sql_instance as c:     
           
        #insert_df_into_table(c,'Employee',employees,delete=True)
        #insert_df_into_table(c,'EmployeeRegional',employee_regions,delete=True)    
        #insert_df_into_table(c,'Regional',regionals,delete=True)     
        #insert_df_into_table(c,'MacroRegional',macroregionals,delete=True)    
        #insert_df_into_table(c,'EmployeeAbsence',absences,delete=True)
        #insert_df_into_table(c,'Form',forms,delete=True)
        #insert_df_into_table(c,'FormField',form_fields,delete=True)
        
        insert_df_into_table(c,'Survey',surveys[0],delete=False)
        insert_df_into_table(c,'SurveyAnswer',surveys[1],delete=False)


        #insert_df_into_table(c,'Visit',visits,delete=True)
        #insert_df_into_table(c,'PointOfSale',pos,delete=True)
        #insert_df_into_table(c,'StatusDay',status_day,delete=True)


    # #print('testing...')
    
    # auth = basic_auth('sistemas','sistemas')

    # server = '172.16.0.7'
    # database = 'Involves_DKT'
    # driver = '{ODBC Driver 17 for SQL Server}'

    # #c = sql_database_connection(server=server,driver=driver,database=database)


    # #format_visits_table(x).to_excel(r'visitas.xlsx')
    # #df = get_employee(auth,1)
    # df = pd.read_csv('survey_detail')
    # df['item_id'] = df['item_id'].fillna(-1)
    # df['item_id'] = pd.to_numeric(df['item_id'], errors='coerce', downcast='integer')
    # df['item_id'] = df['item_id'].replace(-1, '')
    # df = df.fillna('')
    # print(df)

    # df.to_csv(r'C:\lazarus\survey_detail.csv')
    