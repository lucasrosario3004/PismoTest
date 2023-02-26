import requests
import json
import pandas as pd
from datetime import date
import params

currencies = params.currencies
table_columns = ['CURRENCY_DATE']
table_columns.extend(currencies)

def get_currency(start_date, end_date):
    
    #convert date to string
    end_date = end_date.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')
    
    #set api url
    url = "https://api.apilayer.com/currency_data/timeframe?start_date="+start_date+"&end_date="+end_date

    payload = {}

    #set key generated previously
    headers= {
        "apikey": params.apikey
    }

    response = requests.request("GET", url, headers=headers, data = payload)
    status_code = response.status_code

    #parse response returning dataframe
    return create_dataframe(parse_response(response.text))

def parse_response(response):
    
    # Api response structure:

    # "success": true,
    # "historical": true,
    # "date": "2023-02-23",
    # "timestamp": 1677196799,
    # "source": "USD",
    # "quotes": {
    #     "USDAED": 3.673106,
    #     "USDAFN": 88.50795,
    #     "USDALL": 108.579025,
    #     "USDAMD": 390.07967,
    #     "USDANG": 1.801845,
    # ...

    res = json.loads(response)

    response = []
    lista = []

    for k,v in res["quotes"].items():
        lista = [k]
        for currency in currencies:
            lista.append(v[currency])
        response.append(lista)
    
    return response

def create_dataframe(lst): 
    df = pd.DataFrame(lst, columns = table_columns)
    return df
