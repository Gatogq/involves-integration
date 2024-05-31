from base64 import b64encode
import pandas as pd
from src.involves_functions import *
from functools import partial
import time
from src.involves_functions import fetch_involves_data
import requests
from configparser import ConfigParser
import os

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



#endpoint = '/v3/environments/5/employees/187/scheduledvisits'
#endpoint2 = '/v2/environments/5/itinerary'
#endpoint3 = '/v1/1/employeeabsence'
endpoint4 = '/v3/environments/1/surveys?formIds=8'
endpoint5 = '/v1/1/form/formsActivated'
#endpoint6 = '/v1/1/form/8'
endpoint7 = '/v1/1/form/formFields/8'
endpoint8 = '/v3/environments/1/surveys/1816679'

params = {
'startDate':'2024-02-01',
'endDate':'2024-02-01',
'size':200

}

params2 = {
    'date':'2024-02-01'
}

start = time.time()

x = requests.get(f'{url}{endpoint4}',headers=headers,params=None)
y=pd.DataFrame(x.json())
y.to_excel(r"C:\Users\gtuyub\OneDrive - DKT de Mexico S.A. de C.V\Escritorio\surveys_test.xlsx")

#print(x.json())

#x_2 =pd.DataFrame(x.json())
#print(x_2)
x_3 = {
    'id': 1816679,
    'label': 'Respondida vía Informar de la App',
    'expirationDate': '2024-03-11',
    'requestDate': '2024-03-11T16:24:49',
    'responseDate': '2024-03-11T16:24:49',
    'status': 2,
    'rescheduled': False,
    'deleted': False,
    'updatedAt': '2024-03-11T16:24:58',
    'projectId': 1,
    'surveyCategoryId': None,
    'pointOfSaleId': 10664,
    'ownerId': 108,
    'typistId': None,
    'surveyScheduleId': None,
    'form': {'id': 8, 'topic': 'PRODUCT'},
    'items': [{'id': 26, 'topic': 'PRODUCT'}],
    'answers': [
        {'id': 27407946, 'value': '', 'score': None, 'question': {'id': 762, 'type': 'PHOTO'}, 'item': None},
        {'id': 27407945, 'value': 'Si', 'score': None, 'question': {'id': 764, 'type': 'SINGLE_CHOICE'}, 'item': {'id': 26, 'topic': 'PRODUCT'}},
        {'id': 27407944, 'value': '1', 'score': None, 'question': {'id': 763, 'type': 'WHOLE_NUMBER'}, 'item': None}
    ]
}

x = pd.DataFrame(x_3['answers'])
print(x)
x.to_excel(r"C:\Users\gtuyub\OneDrive - DKT de Mexico S.A. de C.V\Escritorio\survey1816679.xlsx" ,index=False)
#df = pd.DataFrame(fetch_involves_data(username=username,password=password,endpoint=endpoint,key='items',params=None))

end = time.time()


print('execution time:', end-start)

# SQL Server configuration
config = ConfigParser()
config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.ini')
config.read(config_file)
server = config.get('SQL', 'Server')
database = config.get('SQL', 'Database')
driver = config.get('SQL', 'Driver')
sql_user = config.get('SQL','username')
sql_pass = config.get('SQL','password')

#sql_instance = sql_database_connection(server=server,driver=driver,trusted_connection=False,database=database,user=sql_user,password=sql_pass)

#insert_df_into_table(connection=sql_instance,table_name='PointOfSale',df=df,delete=True)
