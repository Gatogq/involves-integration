from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, BIGINT,DATE,DATETIME

engine = create_engine('sqlite:///example.db')

metadata = MetaData()

# Define the table schema
pointofsale = Table(
    'PointOfSale', metadata,
    Column('id', Integer, primary_key=True),
    Column('pointOfSaleBaseId',Integer),
    Column('name', String, nullable=False),
    Column('code', String),
    Column('enabled', Boolean),
    Column('region_name', String),
    Column('chain_name', String),
    Column('pointOfSaleType', String),
    Column('pointOfSaleProfile', String),
    Column('pointOfSaleChannel_name', String),
    Column('address_zipCode', String),
    Column('address_latitude', Float),
    Column('address_longitude', Float),
    Column('deleted', Boolean),
    Column('updated_at',BIGINT)
)

employee = Table(
    'Employee', metadata,
    Column('id',Integer,primary_key=True),
    Column('name',String,nullable=False),
    Column('updated_at',BIGINT)

)

visit = Table(
    'Visit',metadata,
    Column('visit_date',DATE),
    Column('customer_id',Integer),
    Column('employee_name',String),
    Column('visit_status',String),
    Column('check_in',DATETIME),
    Column('check_out',DATETIME)

)



# Create the table in the database
metadata.create_all(engine)
