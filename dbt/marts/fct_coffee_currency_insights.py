import database.snowflake as sn
import params

def generate_coffee_insights():
    sql =   'with metrics as ('\
                'select '\
                    'currency_date, '\
                    'currency_year, '\
                    'volume, '\
                    'close_usd, '\
                    'close_eur, '\
                    'close_clp, '\
                    'close_brl, '\
                    'dense_rank() over (order by volume desc) as rank_volume_by_day, '\
                    'sum(volume) over (partition by CURRENCY_YEAR) as sum_volume_by_year, '\
                    'sum(volume) over (partition by CURRENCY_YEAR_MONTH) as sum_volume_by_year_month, '\
                    'sum(volume) over (partition by CURRENCY_YEAR, CURRENCY_YEAR_MONTH) as sum_volume_by_month_in_year, '\
                    'avg(volume) over (partition by CURRENCY_YEAR) as avg_volume_by_year, '\
                    'avg(volume) over (partition by CURRENCY_YEAR_MONTH) as avg_volume_by_year_month '\
                'from PISMO.PUBLIC.INT_COFFEE_CURRENCY '\
            '), '\
            'ranks as ('\
                'select '\
                    '*,'\
                    'dense_rank() over (order by sum_volume_by_year desc) as rank_volume_by_year, '\
                    'dense_rank() over (order by sum_volume_by_year_month desc) as rank_volume_by_year_month, '\
                    'dense_rank() over (partition by currency_year order by sum_volume_by_month_in_year desc) as rank_volume_by_month_in_year '\
                'from metrics '\
            ') '\
            'select * from ranks'

    df = sn.execute_sql(sql)
    
    return df
