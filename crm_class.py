import csv
import settings

import requests
import uuid
import time


class Odata():
    """Class to connect to TE instance of Dynamics via oData API."""

    def __init__(self, sandbox):
        if sandbox:
            sandbox_flag = 'sandbox'
        else:
            sandbox_flag = ''
        self.crmorg = f'https://togetherenergy{sandbox_flag}.crm11.dynamics.com'  # base url for crm org
        self.clientid = 'd3be38a1-f508-401c-aa24-57db6af0b083'  # application client id
        self.username = settings.odata_username  # username
        self.userpassword = settings.odata_userpassword  # password
        self.tokenendpoint = 'https://login.microsoftonline.com/00b00e7f-ac76-4fd6-95a4-1e59637ce7a0/oauth2/token'  # oauth token endpoint
        self.crmwebapi = f'https://togetherenergy{sandbox_flag}.crm11.dynamics.com/api/data/v9.0'  # full path to web api endpoint
        self.accesstoken = None
        self.crmrequestheaders = {
            'Authorization': None,
            'OData-MaxVersion': '4.0',
            'OData-Version': '4.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
            'Prefer': 'return=representation'
        }

    def get_access_token(self):
        """Get access token for API, must be called before any requests are made to the API."""
        # build the authorization token request
        tokenpost = {
            'client_id': self.clientid,
            'resource': self.crmorg,
            'username': self.username,
            'password': self.userpassword,
            'grant_type': 'password'
        }
        tokenres = requests.post(self.tokenendpoint, data=tokenpost)
        try:
            self.accesstoken = tokenres.json()['access_token']
            self.crmrequestheaders['Authorization'] = 'Bearer ' + self.accesstoken
        except KeyError as e:
            print('Cannot get access token.')

    def get_req(self, entity, top=None, select=None, fltr=None, textquery=None, printprogress=True):
        """Make a GET request to the oData API."""
        if textquery:
            crmwebapiquery = textquery
        else:
            top_param = ''
            select_param = ''
            filter_param = ''
            if top:
                top_param = '$top={0}'.format(top)
            if select:
                select_param = '$select={0} '.format(','.join(select))
            if fltr:
                filter_param = '$filter={0}'.format(fltr)
            params = (top_param, select_param, filter_param)
            param_string = '&'.join([p for p in params if p])
            crmwebapiquery = '/{0}?{1}'.format(entity, param_string)
        results = self.get_all_data(crmwebapiquery, printprogress)
        return results

    def get_page(self, url, attempt=1):
        """Get the next page of results form the oData API. Datasets returned by the API are
        returned in chunks of 5000 records. Where more than 5000 records are to be returned, a link
        for the next page is included."""
        try:
            crmres = requests.get(url, headers=self.crmrequestheaders)
            # pprint(getmembers(crmres))
            crmresults = crmres.json()
            records = crmresults['value']
            next_link = crmresults['@odata.nextLink'] if '@odata.nextLink' in crmresults else None
        except (ValueError, requests.exceptions.ConnectionError):
            self.get_access_token()
            if attempt < 5:
                time.sleep(5)
                attempt += 1
                print('oData failure. retrying - attempt {0}'.format(attempt))
                records, next_link = self.get_page(url, attempt=attempt)
            else:
                return None
        return records, next_link

    def get_all_data(self, api_query, printprogress):
        """Get and collate all of the data returned by a GET request to the oData API. Where
        the results include multiple pages, iterate over the pages and combine the data into one
        list."""
        url = self.crmwebapi + api_query
        records = []
        next_link = url
        while next_link:
            if records and printprogress:
                print(len(records))

            query_results = self.get_page(next_link)
            # for el in query_results:
            #     print(type(el))
            new_records, next_link = query_results[0], query_results[1]
            records = records + new_records
        return records

    def post_req(self, entity, data):
        """Make a POST request to the oData API."""
        crmwebapiquery = '/{0}'.format(entity)
        url = self.crmwebapi + crmwebapiquery
        result = requests.post(
            url,
            headers=self.crmrequestheaders,
            data=str(data)
        )
        return result

    def patch_req(self, entity, record_id, data):
        """Make a PATCH request to the oData API."""
        crmwebapiquery = '/{0}({1})'.format(entity, record_id)
        url = self.crmwebapi + crmwebapiquery
        # print crmwebapiquery
        # pprint(data)
        result = requests.patch(
            url,
            headers=self.crmrequestheaders,
            data=str(data)
        )
        return result

    def del_req(self, entity, record_id):
        """Make a DELETE request to the oData API."""
        crmwebapiquery = '/{0}({1})'.format(entity, record_id)
        url = self.crmwebapi + crmwebapiquery
        response = requests.delete(
            url,
            headers=self.crmrequestheaders
        )
        return response
