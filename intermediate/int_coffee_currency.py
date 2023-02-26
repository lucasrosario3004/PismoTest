import database.snowflake as sn
import params

query = ''

for param in params.currencies:
    query = query + '"Open" * "'+ param + '" as OPEN' + param.replace('USD', '_') + ', '\
    + '"High" * "'+ param + '" as HIGH' + param.replace('USD', '_') + ', '\
    + '"Low" * "'+ param + '" as LOW' + param.replace('USD', '_') + ', '\
    + '"Close" * "'+ param + '" as CLOSE' + param.replace('USD', '_') + ', '\

print(query)
dt = '"Date"'

sql =   'select '\
            + dt + ' as currency_date,'\
            ' "Open" as OPEN_USD,'\
            ' "High" as HIGH_USD,'\
            ' "Low" as LOW_USD,'\
            ' "Close" as CLOSE_USD,'\
            + query + \
            ' "Volume" as VOLUME'\
        ' from pismo.public.coffee as coffee '\
            'inner join pismo.public.currency as currency on coffee."Date" = currency.currency_date'

print(sql)

df = sn.execute_sql(sql)

for line in df:
    print(line)
