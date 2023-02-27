import dbt.intermediate.int_coffee_currency as coffee_currency
import dbt.marts.fct_coffee_currency_insights as insights
import database.snowflake as sn
import seeds.csv_handler as csv_handler

def generate_dbt_insights():
    df = coffee_currency.generate_coffee_currency()
    sn.write_to_db('int_coffee_currency', df, True)

    df = insights.generate_coffee_insights()
    sn.write_to_db('fct_coffee_currency_insights', df, True)
    csv_handler.write_csv('fct_coffee_currency_insights.csv', ';', df)