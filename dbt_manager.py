import dbt.intermediate.int_coffee_currency as coffee_currency
import dbt.marts.fct_coffee_currency_insights as insights
import database.snowflake as sn
import seeds.csv_handler as csv_handler

def generate_dbt_insights():
    df = coffee_currency.generate_coffee_currency()
    sn.write_to_db('int_coffee_currency', df, True)

    df = insights.generate_coffee_rankings()
    sn.write_to_db('fct_coffee_currency_insights', df, True)

    df = insights.generate_max_vol_trade_a_day()
    sn.write_to_db('fct_max_vol_trade_in_a_day', df, True)
    csv_handler.write_csv('maior_volume_cafe_negociado_dia.csv', ';', df)

    df = insights.generate_total_coffee_in_year()
    sn.write_to_db('fct_total_coffee_in_year', df, True)
    csv_handler.write_csv('total_cafe_negociado_por_ano.csv', ';', df)

    df = insights.generate_avg_volume_by_year_and_month()
    sn.write_to_db('fct_avg_vol_by_year_month', df, True)
    csv_handler.write_csv('media_cafe_negociado_por_ano_e_mes.csv', ';', df)
    