import currency_manager as cm
import coffee_manager as coffee

#load currency from api and write to snowflake
cm.load_currency_data()

#load coffee from csv and write to snowflake
coffee.load_coffee_data()
