import currency_manager as cm
import coffee_manager as coffee
import dbt_manager as dbtm
import google_drive.drive as drive
import time

#load currency from api and write to snowflake
print('Loading currency data from API ... ')
cm.load_currency_data()

#load coffee from csv and write to snowflake
print('Loading coffee data from CSV ...')
coffee.load_coffee_data()

#generate intermediate and marts models
print('Generating Insights ...')
dbtm.generate_dbt_insights()

#write files to google drive
print('Uploading to google drive ...')
drive.upload_to_googledrive('maior_volume_cafe_negociado_dia.csv', '1iIP1kwx_AwxKk2pUbl8ekTcqpEp_b_8a')
time.sleep(5)
drive.upload_to_googledrive('total_cafe_negociado_por_ano.csv', '1iCoryvDCE7AHSxjVDbRUxYaVelGc_dny')
time.sleep(5)
drive.upload_to_googledrive('media_cafe_negociado_por_ano_e_mes.csv', '1sgFsuHr8yvjPZrcv98JKbWTdMY8FGJT-')