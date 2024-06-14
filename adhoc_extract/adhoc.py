import os 
import requests
import pandas as pd
from src import project_root
import json
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
            "includeAll": 'true',
            "imcludeImages": 'false',
            "includeGenericArticles": 'false',
            "includeOEMNumbers": 'false'
        }
    }

config.read(CONFIG_FILE)
params = {'api_key': config['techdoc']['api_key']}
payload = create_payload()

def json_to_df(response_json):
    # Flattening the genericArticles
    df_generic_articles = pd.json_normalize(
        response_json,
        record_path='genericArticles',
        meta=['dataSupplierId', 'articleNumber', 'mfrId', 'mfrName', 'searchQuery'],
        record_prefix='genericArticle_',
        errors='ignore'
    )

    # Flattening the oemNumbers
    df_oem_numbers = pd.json_normalize(
        response_json,
        record_path='oemNumbers',
        meta=['dataSupplierId', 'articleNumber', 'mfrId', 'mfrName', 'searchQuery'],
        record_prefix='oem_',
        errors='ignore'
    )

    # Merging the two dataframes on common columns
    df_merged = pd.merge(df_generic_articles, df_oem_numbers, on=['dataSupplierId', 'articleNumber', 'mfrId', 'mfrName', 'searchQuery'], how='outer')

    return df_merged


if __name__ == "__main__":
    
    s = requests.Session()

    response_list = []
    counter = 0
    for oem in [
        'A2128300318' 
        #  '04465-33480', '04466-33230', 
        #  '04465-33240', '04466-33090'
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
    # with open('sample_out.json', 'w') as f:
    #     json.dump(response_list[0], f, ensure_ascii=False)
    # response_list = map(lambda x: json_to_df(x), response_list)
    # response_df = pd.concat(response_list).reset_index(drop=True)
    # response_df.to_csv('sample2.csv')

