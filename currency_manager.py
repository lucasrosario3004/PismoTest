import api.currency as currency
import database.snowflake as snowflake
from datetime import date, timedelta

def load_currency_data():
    #If true will delete table if exists and recreate. Used at first time run.
    drop_table = True

    #load this year until yesterday
    end_date = date.today() + timedelta(days=-1)
    start_date = date(2023, 1, 1)
    #call currency api
    df = currency.get_currency(start_date, end_date)
    #write to snowflake
    snowflake.write_to_db('currency', df, drop_table)

    drop_table = False

    #we need to load 2019 because coffee.csv has 2019 data
    #load 2019, 2020, 2021, 2022
    x = range(1,5)
    for i in x:
        end_date = date(2023-i,12,31)
        start_date = date(2023-i,1,1)
        df = currency.get_currency(start_date, end_date)
        snowflake.write_to_db('currency', df, drop_table)

    #run again to load today - this will be used to run daily by airflow
    start_date = date.today()
    end_date = date.today()
    df = currency.get_currency(start_date, end_date)
    snowflake.write_to_db('currency', df, drop_table)
