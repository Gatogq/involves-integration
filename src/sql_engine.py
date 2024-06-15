from sqlalchemy import create_engine, MetaData, Table, select

class SQLServerEngine:

    def __init__(self,engine_type,server=None,database=None):
    
        self.server = server
        self.database = database
        if engine_type == 'mssql':
            self.connection_url = f'mssql+pyodbc://{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        elif engine_type == 'sqlite':
            self.connection_url = f'sqlite:///{database}'

        self.engine = create_engine(self.connection_url)


    def execute_query(self,query):
        
        with self.engine.connect() as c:

            r = c.execute(query)

            try:
                return r.fetchall()
            
            except:
                
                print('La consulta se ejecutó correctamente sin devolver filas.')

                return None
    
    def get_set_of_unique_values(self,table,column):

        r = self.execute_query(f'''
                                  SELECT DISTINCT
                                  {column} FROM {table}''')
        
        return set([row[0] for row in r])
    
    def bulk_insert_from_df(self, table_name, df):
        
        metadata = MetaData()
        table = Table(table_name, metadata, autoload=True, autoload_with=self.engine)

        d = df.to_dict(orient='records')

        try:

            with self.engine.connect() as connection:
                connection.execute(table.insert(), d)

            print(f'Se insertaron correctamente los nuevos registros en la tabla {table_name}. Se añadieron {len(df)} registros')

        except Exception as e:
            raise(f'Error al intentar actualizar la tabla {table_name}: {e}')

        
    
    def update_records_from_df(self, table_name, df, primary_key):
        
        metadata = MetaData()
        table = Table(table_name, metadata, autoload=True, autoload_with=self.engine)

        try:
            with self.engine.connect() as connection:

                    update_data = df.to_dict(orient='records')
                    for data in update_data:
                        statement = table.update().where(table.c[primary_key] == data[primary_key]).values(data)
                        connection.execute(statement)


            print(f'Se actualizaron correctamente los registros de la tabla {table_name}. Se modificaron {len(df)} registros.')

        except Exception as e:

            raise(f'Error al intentar actualizar registros en la tabla {table_name}: {e}')

    def get_columns_from_table(self, table):

        if self.type == 'mssql':

            r = self.execute_query(f'''SELECT COLUMN_NAME
                                FROM INFORMATION_sCHEMA.COLUMNS
                               WHERE TABLE_NAME = '{table}'
                                 ''')
            return [row[0] for row in r]
            
        elif self.type == 'sqlite':

            r = f'''
                PRAGMA table_info({table}
            '''
            return [row['name'] for row in r]
    
    def get_last_update_timestamp(self, table,time_column):

        r = self.execute_query(f"SELECT MAX({time_column}) FROM {table}")
        if r:
            return r[0][0] or 0
        else:
            return 0 
    
    def select_values(self,table,column):

        metadata = MetaData()
        table = Table(table,metadata,autoload=True,autoload_with=self.engine)

        query = select([table.c[column]])
        r = self.execute_query(query)

        return [row[0] for row in r]
    