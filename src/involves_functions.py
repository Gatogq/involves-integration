from time import sleep
from datetime import datetime
from pandas import read_excel, to_datetime, to_numeric, isna
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from os import remove
from os.path import join
from pyodbc import connect
from base64 import b64encode
import requests
from tempfile import mkdtemp
import numpy as np
import pandas as pd
import pyautogui
import os


def fetch_involves_data(username,password,endpoint,key='items',params=None):

    print(f"Descargando información de: {endpoint}")
    url = 'https://dkt.involves.com/webservices/api'
    credentials = f'{username}:{password}'
    encoded = b64encode(credentials.encode()).decode()

    headers = {
        'Authorization':f'Basic {encoded}',
        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
        }
    rows =[]
    page = 1
    totalPages = None

    while totalPages is None or page < totalPages + 1:
        try:
            response = requests.get(f'{url}{endpoint}?page={page}',headers=headers,params=params)
            response.raise_for_status()
            items = response.json().get(key,[])
            if not items:
                break
            rows.extend(items)
            print(items)
            totalPages = response.json().get('totalPages')
            page += 1
        except Exception as e:
            
            print(f"No se encontró información en: {endpoint} : {e}")
            break

    return rows

def involves_date(date):
    day = str(date.day)
    month = str(date.month)
    year = str(date.year)[-2:]
    formatted_date = f"{day}/{month}/{year}"
    return str(formatted_date)

def timestamp_print(string):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'[{timestamp}] {string}')


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

def extract_employee_data(rows):
    
    data = []
    for row in rows:
        table = {
            'id': row.get('id'),
            'name': row.get('name'),
            'IdCard': row.get('nationalIdCard2'),
            'Group_Id': row.get('userGroup', {}).get('name') if row.get('userGroup') and row['userGroup'].get('name') else None,
            'Leader_id': row.get('employeeEnvironmentLeader', {}).get('id') if row.get('employeeEnvironmentLeader') and row['employeeEnvironmentLeader'].get('id') else None,
            'email': row.get('email'),
            'enabled': row.get('enabled'),
            'fieldTeam': row.get('fieldTeam'),
            'Phone': row.get('cellPhone'),
            'Hierarchy': row.get('userGroup', {}).get('hierarchyLevel') if row.get('userGroup') and row['userGroup'].get('hierarchyLevel') else None,
        }


        data.append(table)

    for user in data:
        if user.get('id') == 179:
            data.remove(user)
        break  
    return data


def extract_pos_data(rows):

    data = []
    for row in rows:
        table = {
                
        'id':row.get('pointOfSaleBaseId'),
        'name':row.get('name'),
        'code':row.get('code'),
        'phone':row.get('phone'),
        'enabled':row.get('enabled'),
        'region_name':row.get('region',{}).get('name') if row.get('region') and row['region'].get('name') else None,
        'pointOfSaleType_id':row.get('pointOfSaleType',{}).get('id') if row.get('pointOfSaleType') and row['pointOfSaleType'].get('id') else None,
        'pointOfSaleProfile_id':row.get('pointOfSaleProfile',{}).get('id') if row.get('pointOfSaleProfile') and row['pointOfSaleProfile'].get('id') else None,
        'pointOfSaleChannel_id':row.get('pointOfSaleChannel',{}).get('id') if row.get('pointOfSaleChannel') and row['pointOfSaleChannel'].get('id') else None,
        'address_zipcode':row.get('address',{}).get('zipcode') if row.get('address') and row['address'].get('zipcode') else None,
        'address_city_name':row.get('address',{}).get('city',{}).get('name') if row.get('address') and row['address'].get('city') else None,
        'address_city_state_name':row.get('address',{}).get('city',{}).get('state',{}).get('name') if row.get('address') and row['address'].get('city') and row['address']['city'].get('state') else None,
        'address_latitude':row.get('address',{}).get('latitude') if row.get('address') and row['address'].get('latitude') else None,
        'address_longitude':row.get('address',{}).get('longitude') if row.get('address') and row['address'].get('longitude') else None   

        }
        data.append(table)
    return data

def extract_reference_table(rows):

    data = []
    for row in rows:
        table = {
            
        'id':row.get('id'),
        'name':row.get('name'),

            }
        data.append(table)
    return data

def download_visits(username,password,date,env,download_folder=None,headless_mode=False,file_name ='informe-gerencial-visitas.xlsx'):
    
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
        if env == 1:
            page_id = 'czEU~2FKSKS0bUKeGe5uBOwQ=='

        if env == 5:
            page_id = 'czEU~2FKSKS0bUKeGe5uBOwQ=='

        driver.get(f'https://dkt.involves.com/webapp/#!/app/{page_id}/paineldevisitas')
        
        #Esperar el filtro de fecha
        input_element = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'c-input'))
            )
        input_element.clear()
        input_element.send_keys(involves_date(date))
        sleep(1)
        button_element = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.XPATH,'//ap-button[@identifier="searchAggregatorFilter"]')
            ))
        button_element.click()
        sleep(6)
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

        sleep(10)
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

    return df[['Fecha de la visita','ID del PDV','Regional','Tipo de check-in','Primer check-in manual','Último check-out manual']]

            
            
def insert_dict_into_table(connection,table_name,data,delete=False):
    with connection.cursor() as cursor:
        try:
            if not data:
                print(f'No se encontraron datos que añadir a la tabla: {table_name}.')
                return
            if delete:
                delete_query = f'DELETE FROM {table_name}'
                cursor.execute(delete_query)
            
            placeholders = ', '.join(['?' for _ in data[0].values()])
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

            for row in data:
                values = list(row.values())
                cursor.execute(insert_query, values)

            connection.commit()
            records = len(data)
            print(f'Se actualizó correctamente la tabla: {table_name}. Se escribieron {records} registros.')

        except Exception as e:
            connection.rollback()
            print(f"Se obtuvo el siguiente error al intentar actualizar la tabla {table_name}: {e}")

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


def extract_employee_region_data(employees):
    data = []
    environment = 5
    regions_endpoint = f'/v3/environments/{environment}/regionals'
    username = 'sistemas'
    password = 'sistemas'
    url = 'https://dkt.involves.com/webservices/api'
    credentials = f'{username}:{password}'
    encoded = b64encode(credentials.encode()).decode()
    headers = {
        'Authorization':f'Basic {encoded}',
        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
    }
    for employee in employees:
        employee_id = employee['id']
        print(employee_id)
        region = requests.get(f'{url}{regions_endpoint}?employeeId={employee_id}',headers=headers,params=None)
        region.raise_for_status()
        region_data = region.json().get('items')




        for row in region_data:
        
            table = {
                'employeeId': employee_id,
                'regionId': row.get('id')
            }

            data.append(table)
            #print(data)

    return pd.DataFrame(data)

def extract_region_data(rows):
    data = []
    for row in rows:
        table = {
            
        'id':row.get('id'),
        'name':row.get('name'),
        'macroregionId':row.get('macroregional',{}).get('id') if row.get('macroregional') and row['macroregional'].get('id') else None,


            }
        data.append(table)
    return data


def extract_macroregion_data(regions):
    data = []
    environment = 5
    macroregions_endpoint = f'/v3/environments/{environment}/macroregionals'
    username = 'sistemas'
    password = 'sistemas'
    url = 'https://dkt.involves.com/webservices/api'
    credentials = f'{username}:{password}'
    encoded = b64encode(credentials.encode()).decode()
    headers = {
        'Authorization':f'Basic {encoded}',
        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
    }
    for region in regions:
        macroregion_id = region['macroregionId']
        print(macroregion_id)
        macroregion = requests.get(f'{url}{macroregions_endpoint}/{macroregion_id}',headers=headers,params=None)
        macroregion.raise_for_status()
        macroregion_data = macroregion.json()
        print(macroregion_data)


        data.append(macroregion_data)
            #print(data)

    return pd.DataFrame(data)


def open_edge_with_url(url):
    
    command = 'start microsoft-edge:' + url
    os.system(command)



def click_b(x, y):
    pyautogui.moveTo(x, y)
    pyautogui.click()


def update_sheet(x,y,tries):

    click_b(650, 160)

    for _ in range(tries):
        click_b(x,y)
        click_b(119,621)
        click_b(241,247)
        print(f"updating sheet at coordinates {x},{y}...")
        sleep(14)
        print("updated successfully.")
        pyautogui.press("esc")
        print(f"{_} attempt")
