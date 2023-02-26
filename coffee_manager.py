import api.currency as currency
import database.snowflake as snowflake
from datetime import date, timedelta
import seeds.csv_loader as loader

def load_coffee_data():
    df = loader.read_csv('./seeds/coffee.csv',',')
    snowflake.write_to_db('coffee', df, True)
