import database.snowflake as sn
import params

def generate_coffee_rankings():
    sql =   'with metrics as ('\
                'select '\
                    'currency_date, '\
                    'currency_year, '\
                    'currency_year_month, '\
                    'volume, '\
                    'close_usd, '\
                    'close_eur, '\
                    'close_clp, '\
                    'close_brl, '\
                    'dense_rank() over (order by volume desc) as rank_volume_by_day, '\
                    'sum(volume) over (partition by CURRENCY_YEAR) as sum_volume_by_year, '\
                    'sum(volume) over (partition by CURRENCY_YEAR_MONTH) as sum_volume_by_year_month, '\
                    'avg(volume) over (partition by CURRENCY_YEAR) as avg_volume_by_year, '\
                    'avg(volume) over (partition by CURRENCY_YEAR_MONTH) as avg_volume_by_year_month '\
                'from PISMO.PUBLIC.INT_COFFEE_CURRENCY '\
            '), '\
            'ranks as ('\
                'select '\
                    '*,'\
                    'dense_rank() over (order by sum_volume_by_year desc) as rank_volume_by_year, '\
                    'dense_rank() over (order by sum_volume_by_year_month desc) as rank_volume_by_year_month, '\
                    'dense_rank() over (partition by currency_year order by sum_volume_by_year_month desc) as rank_volume_by_month_in_year '\
                'from metrics '\
            ') '\
            'select * from ranks'

    return sn.execute_sql(sql)

def generate_max_vol_trade_a_day():
    sql =   'select '\
                'currency_date as dia_maior_volume, '\
                'volume as maior_volume_cafe_dia, '\
                'close_usd as cotacao_dolar_dia_maior_volume, '\
                'close_eur as cotacao_euro_dia_maior_volume, '\
                'close_clp as cotacao_peso_dia_maior_volume, '\
                'close_brl as cotacao_real_dia_maior_volume '\
            'from pismo.public.fct_coffee_currency_insights '\
            'where RANK_VOLUME_BY_DAY = 1'\

    return sn.execute_sql(sql)

def generate_total_coffee_in_year():
    sql =   'select '\
                'currency_year as ano, '\
                'sum(volume) as total_cafe_negociado_ano, '\
                'round(avg(close_usd),2) as media_anual_cotacao_dolar, '\
                'round(avg(close_eur),2) as media_anual_cotacao_euro, '\
                'round(avg(close_clp),2) as media_anual_cotacao_peso, '\
                'round(avg(close_brl),2) as media_anual_cotacao_real '\
            'from pismo.public.int_coffee_currency  '\
            'group by 1  '\
            'order by 1  '\

    return sn.execute_sql(sql)

def generate_avg_volume_by_year_and_month():
    sql =   'select '\
                'currency_year as ano, '\
                'sum(volume) as total_cafe_negociado_ano, '\
                'round(avg(close_usd),2) as media_anual_cotacao_dolar, '\
                'round(avg(close_eur),2) as media_anual_cotacao_euro, '\
                'round(avg(close_clp),2) as media_anual_cotacao_peso, '\
                'round(avg(close_brl),2) as media_anual_cotacao_real '\
            'from pismo.public.int_coffee_currency  '\
            'group by 1  '\
            'order by 1  '\

    return sn.execute_sql(sql)
