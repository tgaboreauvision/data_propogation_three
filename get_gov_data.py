import csv
import pyodbc
import sys
import settings


if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    date = input("Please enter the date of the file : ")


def open_csv(filename, fieldnames=None):
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=fieldnames)
        return [row for row in reader], reader.fieldnames


def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            try:
                writer.writerow(row)
            except UnicodeEncodeError:
                for k, v in row.iteritems():
                    if v:
                        try:
                            row[k] = ''.join([char if ord(char) < 128 else '?' for char in v])
                        except TypeError:
                            pass
                writer.writerow(row)


def crm_query(query):
    conn = pyodbc.connect(
        f'DSN={settings.tog_dsn};DATABASE=Customer;UID={settings.tog_uid};PWD={settings.tog_pwd}')
    cur = conn.cursor()
    cur.execute(query)
    columns = [col[0] for col in cur.description]
    output = []
    for row in cur.fetchall():
        rowDict = {v: row[i] for i, v in enumerate(columns)}
        output.append(rowDict)
    conn.close()
    return output, columns


def get_gov_data(mpxn, field, table):
    query = "select * from %s where %s = '%s'" % (table, field, mpxn)
    data, fieldnames = crm_query(query)
    return data, fieldnames


quotes, in_fields = open_csv(f'In_New_Customer_{date}.csv')


ecoes_fields = [
    u'Address1',
    u'Address2',
    u'Address3',
    u'Address4',
    u'Address5',
    u'Address6',
    u'Address7',
]


xos_fields = [
    u'MSN',
    u'SubBuilding',
    u'BuildingName',
    u'BuildingNumber',
    u'PrincipalStreet',
    u'DependentLocality',
    u'PostTown',
]

out_data = []

print(len(quotes))

for i, quote in enumerate(quotes):
    print(i)
    for field in ecoes_fields:
        quote['gov_ecoes_' + field] = None
    for field in xos_fields:
        quote['gov_xos_' + field] = None

    mpan = quote['MPAN_(ELECTRICITY)']
    if mpan:
        ecoes_data, fields = get_gov_data(mpan, 'MPAN', 'ECOES.MPAN')
        if not ecoes_data:
            print('No Gov data found for %s' % mpan)
            quote['mpan_data_found'] = 'No'
        else:
            for field in ecoes_fields:
                quote['gov_ecoes_' + field] = ecoes_data[0][field]
        if len(ecoes_data) > 1:
            print('More than 1 gov result found for %s' % mpan)

    mprn = quote['MPRN_(GAS)']
    if mprn:
        xos_data, fields = get_gov_data(mprn, 'MPRN', 'XOSERVE.MPRN')
        if not xos_data:
            print('No Gov data found for %s' % mprn)
            quote['mprn_data_found'] = 'No'
        else:
            for field in xos_fields:
                quote['gov_xos_' + field] = xos_data[0][field]
        if len(xos_data) > 1:
            print('More than 1 gov result found for %s' % mprn)

    out_data.append(quote)

ecoes_out_fields = ['gov_ecoes_' + field for field in ecoes_fields]
xos_out_fields = ['gov_xos_' + field for field in xos_fields]
all_out_fields = in_fields + ecoes_out_fields + xos_out_fields + ['mpan_data_found',
                                                                  'mprn_data_found']

write_csv(f'enriched_data_{date}.csv', out_data, all_out_fields)
