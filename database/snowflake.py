from snowflake.connector.pandas_tools import pd_writer
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
import params
from snowflake.connector.errors import DatabaseError

#Create connection to Snowflake using account and user
def connect_to_db():
    account_identifier = params.account_identifier
    user = params.user
    password = params.password
    database_name = params.database_name
    schema_name = params.schema_name

    conn_string = f"snowflake://{user}:{password}@{account_identifier}/{database_name}/{schema_name}"
    engine = create_engine(conn_string)

    print("****connected to snowflake****")

    return engine

def write_to_db(table_name, df, drop_table):

    try:
        #Connect to database
        engine = connect_to_db()

        if not drop_table:
            if_exists = 'append'
        else:
            if_exists = 'replace'

        #Write the data to Snowflake, using pd_writer to speed up loading
        with engine.connect() as con:
            df.to_sql(name=table_name.lower(), con=engine, schema='PUBLIC', if_exists=if_exists, index=False, method=pd_writer)
        
        print('*****wrote to table ' +table_name.upper()+ ' in snowflake*****')
    except DatabaseError as db_ex:
        print(db_ex)
    finally:
        engine.dispose()

def execute_sql(sql):
    df=[]
    try:
        #Connect to database
        engine = connect_to_db()
        rows = 0
        df = pd.read_sql_query(sql, engine)
        #remove_quotes and uppercase column names
        df.columns = df.columns.to_series().apply(lambda x: x.replace('"', '').upper())
        print('*****query executed in snowflake*****')
    except DatabaseError as db_ex:
        print(db_ex)
    finally:
        engine.dispose()
    
    return df
