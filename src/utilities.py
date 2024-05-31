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
from pyodbc import connect
from base64 import b64encode
from tempfile import mkdtemp

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


def extract_json_keys(d, keys):

    try:
        return reduce(lambda d, k: d.get(k, {}), keys.split('.'), d)
    
    except (AttributeError, TypeError):
        return None


def extract_nested_keys(data, keys):
    result = {}

    for key in keys:
        nested_keys = key.split('.') 
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


def involves_date(date):

    day = str(date.day)
    month = str(date.month)
    year = str(date.year)[-2:]

    formatted_date = f"{day}/{month}/{year}"

    return str(formatted_date)


def set_null_values(value):
    
    if isinstance(value, dict) or value == '':
        return None
    
    return value
