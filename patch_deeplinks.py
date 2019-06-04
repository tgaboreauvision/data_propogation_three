"""Open a CSV file containing account numbers and deeplinks. Find the accounts in Dynamics and
patch them with the deeplinks"""
import csv
from crm_class import Odata

filename = 'deeplink_data_20190325.csv'

"""creates class to access CRM"""
dynamics = Odata(sandbox=False)
dynamics.get_access_token()


def get_account_by_account_number(account_number):
    """Get an account GUID from Dynamics using the account number."""
    accounts = dynamics.get_req('accounts', fltr=f"name eq '{account_number}'")
    if not accounts:
        print(f'no account found with account number: {account_number}')
    elif len(accounts) > 1:
        print(f'more than one account found with account number: {account_number}')
    else:
        return accounts[0]['accountid']


def patch_deeplink(guid, deeplink, attempt=1):
    """Patch an account with a deeplink using the account GUID."""
    data = {'websiteurl': deeplink}
    request = dynamics.patch_req('accounts', guid, data)
    if request.status_code == 200:
        return True
    else:
        if request.reason == 'Unauthorized':
            attempt += 1
            print(f'oData failure - retrying (attempt {attempt})')
            dynamics.get_access_token()
            patch_deeplink(guid, deeplink, attempt)


"""Open data from csv"""
with open(filename, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader]


for i, row in enumerate(data):
    account_number = row['name']
    deeplink = row['deeplink']
    if deeplink:
        guid = row['accountid']
        if guid:
            patched = patch_deeplink(guid, deeplink)
            if patched:
                print(i, f'patched {account_number} ({guid}) with {deeplink}')
    else:
        print(i, f'no deeplink in file for {account_number}')



