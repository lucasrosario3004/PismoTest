import database.snowflake as sn
import params

def generate_coffee_currency():

    query = ''

    for param in params.currencies:
        query = query + '"Open" * "'+ param + '" as OPEN' + param.replace('USD', '_') + ', '\
        + '"High" * "'+ param + '" as HIGH' + param.replace('USD', '_') + ', '\
        + '"Low" * "'+ param + '" as LOW' + param.replace('USD', '_') + ', '\
        + '"Close" * "'+ param + '" as CLOSE' + param.replace('USD', '_') + ', '\

    sql =   'select '\
                '"Date" as CURRENCY_DATE,'\
                ' TO_VARCHAR("Date"::date, \'yyyy\') AS CURRENCY_YEAR,'\
                ' TO_VARCHAR("Date"::date, \'yyyymm\') AS CURRENCY_YEAR_MONTH,'\
                ' "Open" as OPEN_USD,'\
                ' "High" as HIGH_USD,'\
                ' "Low" as LOW_USD,'\
                ' "Close" as CLOSE_USD,'\
                + query + \
                ' "Volume" as VOLUME'\
            ' from pismo.public.coffee as coffee '\
                'inner join pismo.public.currency as currency on coffee."Date" = currency.currency_date'

    return sn.execute_sql(sql)
