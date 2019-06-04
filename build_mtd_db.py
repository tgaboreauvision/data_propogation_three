from to_postgres import db_builder
import settings

# create instance of builder class
new_db = db_builder(settings.local_postgres_db_name, settings.local_postgres_db_password)

# create target db
new_db.create_target_db(drop_existing=False)

new_db.add_source_conn('tecrm', 'mssql', dsn=settings.tog_dsn, user=settings.tog_uid,
                       password=settings.tog_pwd, dbname='customer')



new_db.add_table('sales_account', 'sql', 'tecrm', 'select * from crm.salesaccount')

new_db.add_table('accounts_small', source='odata', entity='accounts')
new_db.add_table('supply_points', source='odata', entity='d4e_energy_supply_points')
new_db.add_table('meters', source='odata', entity='d4e_meters')
new_db.add_table('registers', source='odata', entity='d4e_registers')
new_db.add_table('energy_users', source='odata', entity='new_energy_users')
new_db.add_table('contacts', source='odata', entity='contacts')
new_db.add_table('agreements', source='odata', entity='msdyn_agreements')
new_db.add_table('price_lists', source='odata', entity='pricelevels')
