import requests
import pandas as pd

from time import sleep
from datetime import datetime
from functools import reduce
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from os import remove
from os.path import join
from base64 import b64encode
from tempfile import mkdtemp
from numpy import isnan

def basic_auth(username,password):
    
    credentials = f'{username}:{password}'
    encoded_credentials = b64encode(credentials.encode()).decode()
    
    return f'Basic {encoded_credentials}'


def static_cache_fn(context,parameters):
    return 'static'


def involves_paginated_request(auth,endpoint,key='items',params=None,at_page=None,paginated=True):

    url = 'https://dkt.involves.com/webservices/api'


    headers = {
        'Authorization': auth,
        'X-AGILE-CLIENT': 'EXTERNAL_APP',
        'Accept-Version': '2020-02-26'
        }
    
    rows = []
    page = 1
    totalPages = None

    try:

        print(f'Extrayendo informacion de {endpoint}')

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

                except Exception as e:
                
                    print(f"No se encontró información en: {endpoint} : {e}")
                    break
        else:

            response = requests.get(f'{url}{endpoint}',headers=headers,params=params)
            response.raise_for_status()
            rows = response.json()

        return rows
    
    except Exception as e:
        raise(f'Error al extraer datos de la API de Involves: {e}')

def extract_json_keys(d, keys):

    try:
        return reduce(lambda d, k: d.get(k, {}), keys.split('_'), d)
    
    except (AttributeError, TypeError):
        return None


def extract_nested_keys(data, keys):
    result = {}

    for key in keys:
        nested_keys = key.split('_') 
        temp = data

        for nested_key in nested_keys:
            if nested_key in temp:
                temp = temp[nested_key]

            else:
                temp = None
                break

        result[key] = temp

    return result


def list_from_file_column(file_path,column_index):

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

def last_survey_from_query(sql_connection,form_id,table):

    with sql_connection as conn:

        with conn.cursor() as cursor:

            cursor.execute(f'SELECT MAX(survey_id) AS last_survey FROM {table} WHERE form_id = {form_id}')
            last_survey = cursor.fetchall()
    
    last_survey = last_survey[0][0]

    return last_survey


def format_involves_date(date):

    day = str(date.day)
    month = str(date.month)
    year = str(date.year)[-2:]

    formatted_date = f"{day}/{month}/{year}"

    return str(formatted_date)


def set_null_values(value):
    
    if isinstance(value, dict):

        return None
    
    if value == '':

        return None
    
    try:

        if isnan(value):

            return None
        
    except TypeError:
        pass
    
    return value


def download_visits(username,password,date,env,domain,wait=10,download_folder=None,headless_mode=False,file_name='informe-gerencial-visitas.xlsx'):
    
    if download_folder is None:
        download_folder = mkdtemp()

    visits = join(download_folder,file_name)
    
    try:
        print(f"Iniciando proceso de descarga en https://{domain}.involves.com/login")
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
        driver.get(f'https://{domain}.involves.com/login')
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

        sleep(5)

        #ir al panel de visitas
        if env == 5:
            page_id = 'czEU~2FKSKS0bUKeGe5uBOwQ=='

        if env == 1:
            page_id = 'Mflfx4vR~2FUIfTPLg5S4O8Q=='

        driver.get(f'https://{domain}.involves.com/webapp/#!/app/{page_id}/paineldevisitas')
        
        #Esperar el filtro de fecha
        input_element = WebDriverWait(driver,10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'c-input'))
            )
        input_element.clear()
        input_element.send_keys(format_involves_date(date))
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
            df['Fecha de la visita'] = pd.to_datetime(df['Fecha de la visita'],format="%d/%m/%Y").dt.date
            df['ID del PDV'] = pd.to_numeric(df['ID del PDV'])
            df['Regional'] = df['Regional'].astype(str)
            df['Tipo de check-in'] = df['Tipo de check-in'].astype(str)
            df['Primer check-in manual'] = pd.to_datetime(df['Primer check-in manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')
            df['Último check-out manual'] = pd.to_datetime(df['Último check-out manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')

            df = df[['Fecha de la visita','ID del PDV','Regional','Tipo de check-in',
                     'Primer check-in manual','Último check-out manual']]
        
        if env == 1:
            df['Fecha de la visita'] = pd.to_datetime(df['Fecha de la visita'],format="%d/%m/%Y").dt.date
            df['ID del PDV'] = pd.to_numeric(df['ID del PDV'])
            df['Regional'] = df['Regional'].astype(str)
            df['Tipo de check-in'] = df['Tipo de check-in'].astype(str)
            df['Primer check-in manual'] = pd.to_datetime(df['Primer check-in manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')
            df['Último check-out manual'] = pd.to_datetime(df['Último check-out manual'],format='%d/%m/%Y %H:%M:%S',errors='coerce')

            df = df[['Fecha de la visita','ID del PDV','Empleado','Tipo de check-in','Primer check-in manual',
                     'Último check-out manual','Total de encuestas respondidas','Motivo para no realizar la visita']]

    return df
