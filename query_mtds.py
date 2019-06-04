import pandas as pd
from sqlalchemy import create_engine
import settings


def postgres_to_csv(engine_string, query, filename):
    engine = create_engine(engine_string)
    df = pd.read_sql_query(query, con=engine)
    df.to_csv(f'data\\{filename}')


engine_string = f'postgresql://postgres:{settings.local_postgres_db_password}@localhost:5432/{settings.local_postgres_db_password}'


elec_query = """
select
	a.name,
	sp.d4e_mpxn,
	m.d4e_serial_number,
	r.d4e_meterregisterid,
	r.d4e_numberofdials,
	r.d4e_timepatternregime,
	sp.d4e_estimatedusage
from
		accounts_small a
	right join
		supply_points sp
		on a.accountid = sp._d4e_accountnumber_value
	left join
		meters m
		on sp.d4e_energy_supply_pointid = m._d4e_esp_meter_value and m.statuscode = 1
	left join
		registers r
		on m.d4e_meterid = r._d4e_meterregisters_value and r.statuscode = 1
where
	sp.d4e_fueltype = 493030000
"""

gas_query = """
select
	a.name,
	sp.d4e_mpxn,
	m.d4e_serial_number,
	r.d4e_meterregisterid,
	r.d4e_numberofdials,
	r.d4e_timepatternregime,
	sp.d4e_estimatedusage,
	m.d4e_nativeunittype
from
		accounts_small a
	right join
		supply_points sp
		on a.accountid = sp._d4e_accountnumber_value
	left join
		meters m
		on sp.d4e_energy_supply_pointid = m._d4e_esp_meter_value and m.statuscode = 1
	left join
		registers r
		on m.d4e_meterid = r._d4e_meterregisters_value and r.statuscode = 1
where
	sp.d4e_fueltype = 493030001
"""

postgres_to_csv(engine_string, elec_query, 'elec_mtds.csv')
postgres_to_csv(engine_string, gas_query, 'gas_mtds.csv')

