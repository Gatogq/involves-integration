from configparser import ConfigParser
from datetime import datetime
from functools import partial
from src.involves_functions import *
import os




def update_involves_db(username,password,server,driver,database):


    instance = partial(fetch_involves_data,username,password)

    employees = extract_employee_data(instance("/v1/5/employeeenvironment"))
    points_of_sale = extract_pos_data(instance("/v1/5/pointofsale"))
    types = extract_reference_table(instance("/v1/pointofsaletype/find",key='itens'))  
    #profiles = extract_reference_table(instance("/v1/5/pointofsaleprofile/find"))
    channels = extract_reference_table(instance("/v3/pointofsalechannels"))
    visits = download_visits(username,password,datetime(2023,11,1),env=5,headless_mode=False)
    visits_df = format_visits_table(visits)


    #SQL connection
    sql_instance = sql_database_connection(server=server,driver=driver,trusted_connection=True,database=database)

    with sql_instance as conn:        
        insert_dict_into_table(conn,'POSChannels',channels,delete=True)        
        insert_dict_into_table(conn,'POSTypes',types,delete=True)        
        #insert_dict_into_table(conn,'POSProfiles',profiles,delete=True)        
        insert_dict_into_table(conn,'Employee',employees,delete=True)        
        insert_dict_into_table(conn,'PointOfSale',points_of_sale, delete=True)
        insert_df_into_table(conn,'Visit',visits_df,delete=True)

def main():

#-----------------------
    config = ConfigParser()
    config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'config.ini')
    config.read(config_file)
    # Involves API credentials
    username = config.get('Credentials', 'Username')
    password = config.get('Credentials', 'Password')
    # SQL Server config
    server = config.get('SQL', 'Server')
    database = config.get('SQL', 'Database')
    driver = config.get('SQL', 'Driver')
#----------------------

    update_involves_db(username=username,password=password,server=server,driver=driver,database=database)

if __name__ == "__main__":

    main()  
