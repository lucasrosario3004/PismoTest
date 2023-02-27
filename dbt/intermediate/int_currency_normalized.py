import database.snowflake as sn
import params

def generate_currency_normalized():

    query = ''

    for param in params.currencies:
        query = query + '"Open" * "'+ param + '" as OPEN' + param.replace('USD', '_') + ', '\
        + '"High" * "'+ param + '" as HIGH' + param.replace('USD', '_') + ', '\
        + '"Low" * "'+ param + '" as LOW' + param.replace('USD', '_') + ', '\
        + '"Close" * "'+ param + '" as CLOSE' + param.replace('USD', '_') + ', '\

    sql =   'with min_max as ('\
                'select '\
                    'min(usdeur) as min_usdeur, '\
                    'max(usdeur) as max_usdeur, '\
                    'min(usdclp) as min_usdclp, '\
                    'max(usdclp) as max_usdclp, '\
                    'min(usdbrl) as min_usdbrl, '\
                    'max(usdbrl) as max_usdbrl '\
                'from pismo.public.currency '\
            '), '\
            'currency as ( '\
                'select * from pismo.public.currency '\
            ') '\
            'select '\
                'currency_date, '\
                'usdeur, '\
                'usdclp, '\
                'usdbrl, '\
                '(usdeur-min_usdeur) / (max_usdeur-min_usdeur) as usdeur_norm, '\
                '(usdclp-min_usdclp) / (max_usdclp-min_usdclp) as usdclp_norm, '\
                '(usdbrl-min_usdbrl) / (max_usdbrl-min_usdbrl) as usdbrl_norm '\
            'from currency '\
            'cross join min_max '\
                
    return sn.execute_sql(sql)
