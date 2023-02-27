import api.currency as currency
import database.snowflake as snowflake
from datetime import date, timedelta
import seeds.csv_handler as loader

def load_coffee_data():
    df = loader.read_csv('./seeds/coffee.csv',',')
    
    #remove_quotes from column names
    df.columns = df.columns.to_series().apply(lambda x: x.replace('"', ''))

    snowflake.write_to_db('coffee', df, True)
