import os 
import requests
import pandas as pd
from src import project_root
from configparser import ConfigParser

CONFIG_FOLDER = os.path.join(project_root,'config')
CONFIG_FILE = os.path.join(CONFIG_FOLDER,'config.ini')
URL = 'https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint'

config = ConfigParser()

def create_payload() -> dict:
    return {
        "getArticles": {
            "articleCountry": "AE",
            "provider": "22610",
            "searchQuery": "",
            "searchType": 1,
            "lang": "en",
            "perPage": 100,
            "page": 1,
            "includeAll": 'false',
            "imcludeImages": 'false',
            "includeGenericArticles": 'false',
            "includeOEMNumbers": 'true'
        }
    }

config.read(CONFIG_FILE)
params = {'api_key': config['techdoc']['api_key']}
payload = create_payload()

def json_to_df(response_json):
    return pd.json_normalize(
        data = response_json, 
        record_path='oemNumbers', 
        meta=['articleNumber', 'mfrId', 'mfrName', 'searchQuery'],
        meta_prefix='afterMarket'
        )


if __name__ == "__main__":
    
    s = requests.Session()

    response_list = []
    counter = 0
    for oem in [
         '04466-06100', '04465-33480', '04466-33230', 
         '04465-33240', '04466-33090'
         ]:
        page = 1
        oemQuery = oem
        while True:
            try:
                with s as session:
                    payload['getArticles']['searchQuery'] = oemQuery
                    payload['getArticles']['page'] = page
                    response = session.post(url=URL, params=params, json=payload)
                    if response.status_code == 200 and len(response.json()['articles']) > 0:
                        response_json = response.json()['articles']
                        for item in response_json: item['searchQuery'] = oemQuery
                        response_list.append(response_json)
                        counter += len(response_json)
                        page += 1
                    elif response.status_code == 200 and len(response.json()['articles']) == 0:
                        break
                    else:
                        print(response.json())
                        break
            except requests.RequestException as e:
                    print(f"Request failed: {e}")
                    break

    print(response_list[0])
    # response_list = map(lambda x: json_to_df(x), response_list)
    # response_df = pd.concat(response_list).reset_index(drop=True)
    # response_df.drop(['referenceTypeKey', 'referenceTypeDescription'], axis=1)
    # response_df.to_csv('sample.csv')
