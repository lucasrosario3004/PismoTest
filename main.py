import currency_manager as cm
import coffee_manager as coffee
import dbt_manager as dbtm
import google_drive.drive as drive

# #load currency from api and write to snowflake
# print('Loading currency data from API ... ')
# cm.load_currency_data()

# #load coffee from csv and write to snowflake
# print('Loading coffee data from CSV ...')
# coffee.load_coffee_data()

# #generate intermediate and marts models
# print('Generating Insights ...')
# dbtm.generate_dbt_insights()

print('Uploading to google drive ...')
drive.upload_to_googledrive('fct_coffee_currency_insights.csv')