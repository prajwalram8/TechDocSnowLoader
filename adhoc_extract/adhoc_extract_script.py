import os
import requests
import pandas as pd
from datetime import datetime as dt
from configparser import ConfigParser
from concurrent.futures import ThreadPoolExecutor, as_completed

project_root = '.'

CONFIG_FOLDER = os.path.join(project_root,'config')
CONFIG_FILE = os.path.join(CONFIG_FOLDER,'config.ini')

config = ConfigParser()
config.read(CONFIG_FILE)

# Read the reference file
oem_iam_df = pd.read_excel('adhoc_extract/OEM-IAM.xlsx', sheet_name='Sheet1')

print(f"Shape of the loaded dataframe {oem_iam_df.shape}\n")
print(f"Distribution of item ids: \n{oem_iam_df['Brand'].value_counts()}")

# Custom UDF Functions for use in script

def create_payload(searchType, includeAll='false',includeImages='false',includeGenericArticles='true',includeOEMNumbers='true' ) -> dict:
    """
    Creates payload dict object based on search type
    1: Input Query is an OEM Brand
    0: Input Query is an IAM Brand
    99: Get based on partial match
    """
    return {
        "getArticles": {
            "articleCountry": "AE",
            "provider": "22610",
            "searchQuery": "",
            "searchType": searchType,
            "lang": "en",
            "perPage": 100,
            "page": 1,
            "includeAll": includeAll,
            "imcludeImages": includeImages,
            "includeGenericArticles": includeGenericArticles,
            "includeOEMNumbers": includeOEMNumbers
        }
    }

def json_to_df(response_json):
    """
    Converts json objects into pandas dataframe
    """
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

def query_oem(oem, url, payload, params):
    oemQuery = oem
    s = requests.Session()
    response_list = []
    no_response_list = []
    problem_items_list = []
    counter = 0
    page = 1
    while True:
        try:
            with s as session:
                payload['getArticles']['searchQuery'] = oemQuery
                payload['getArticles']['page'] = page
                response = session.post(url=url, params=params, json=payload)
                try:
                    if response.status_code == 200 and len(response.json()['articles']) > 0:
                        response_json = response.json()['articles']
                        for item in response_json: item['searchQuery'] = oemQuery
                        response_list.append(response_json)
                        counter += len(response_json)
                        page += 1
                    elif response.status_code == 200 and len(response.json()['articles']) == 0:
                        break
                    else:
                        print(f"Error {response.status_code}: {response.text}")
                        if page == 1:
                            problem_items_list = [
                                [
                                    {
                                        'searchQuery':oem, 
                                        'Status Code':response.status_code,
                                        'Error': response.text
                                    }
                                ]
                            ]
                        break
                except KeyError:
                    break
        except requests.RequestException as e:
                print(f"Request failed: {e}")
                break
        
    if len(response_list) > 0:
        response_list = list(map(lambda x: json_to_df(x), response_list))
    elif len(response_list) == 0:
        no_response_list = [[{'searchQuery':oem}]]
        no_response_list = list(map(lambda x: pd.json_normalize(x), no_response_list))
    elif len(problem_items_list) > 0:
        problem_items_list= list(map(lambda x: pd.json_normalize(x), problem_items_list))
        
    return response_list, no_response_list, problem_items_list

def process_batch(batch, url, params, payload):
    response_list, no_response_list, problem_items_list = [], [], []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sku = {executor.submit(query_oem, oem, url, payload, params): oem for oem in batch}
        for future in as_completed(future_to_sku):
            response, no_response, problem_items = future.result()
            response_list.extend(response)
            no_response_list.extend(no_response)
            problem_items_list.extend(problem_items)
    return response_list, no_response_list, problem_items

def save_data_in_batches(articles, no_response_list, problem_items, index, data_stage_location):
    try:
        if not articles.empty:
            save_to_csv(articles, "oem_matches", index, data_stage_location)
        if not no_response_list.empty:
            save_to_csv(no_response_list, "no_responses", index, data_stage_location)
        if not problem_items.empty:
            save_to_csv(problem_items, "errors", index, data_stage_location)
    except Exception as e:
        print(f"Exception occurred during saving CSV: {e}")

def save_to_csv(df, file_type, index, data_stage_location):
    start, end = max(0, index - 5000), index
    dt_stamp = dt.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(data_stage_location,file_type,f"{file_type}_{start}_{end}_{dt_stamp}.csv")
    df.to_csv(file_path, index=False, encoding='utf-8')

# OEM Extraction
oem_list = oem_iam_df[oem_iam_df['Brand'] == 'OEM']['SKU']
batch_size = 5000

payload = create_payload(searchType=1)
params = params = {'api_key': config['techdoc']['api_key']}
url = 'https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint'

for start in range(0, len(oem_list), batch_size):
    end = start + batch_size
    print(f"Dispatching elements between index {start} to {end}")
    batch = oem_list[start:end]
    response_list, no_response_list, problem_items_list = process_batch(batch=batch, url=url, params=params, payload=payload)
    
    # Handling the outputs
    response_df = pd.DataFrame()
    no_response_df = pd.DataFrame()
    problem_items_df = pd.DataFrame()

    if len(response_list) > 0:
        response_df = pd.concat(response_list).reset_index(drop=True)

    if len(no_response_list) > 0:
        no_response_df = pd.concat(no_response_list).reset_index(drop=True)

    if len(problem_items_list) > 0:
        problem_items_df = pd.concat(problem_items_list).reset_index(drop=True)
    
    save_data_in_batches(response_df, no_response_df, problem_items_df, end, 'adhoc_extract/custom_data_extract/oem')
        
print("Extraction completed successfully")

folder_name = 'oem_matches'
path = f'adhoc_extract/custom_data_extract/oem/{folder_name}'
contents = os.listdir(path)
oem_match_df = []

for each in contents:
    if each.endswith('.csv'):
        file_path = f"{os.path.join(path,each)}"
        oem_match_df.append(pd.read_csv(file_path))

oem_match_df = pd.concat(oem_match_df, axis=0)

print(f"Total number of uniques items with match: {oem_match_df['searchQuery'].nunique()}")

folder_name = 'no_responses'
path = f'adhoc_extract/custom_data_extract/oem/{folder_name}'
contents = os.listdir(path)
oem_no_match_df = []

for each in contents:
    if each.endswith('.csv'):
        file_path = f"{os.path.join(path,each)}"
        oem_no_match_df.append(pd.read_csv(file_path))

oem_no_match_df = pd.concat(oem_no_match_df, axis=0)

print(f"Total number of uniques items with match: {oem_no_match_df['searchQuery'].nunique()}")

if oem_match_df['searchQuery'].nunique() + oem_no_match_df['searchQuery'].nunique() == len(oem_list):
    print("All Items Have Been Accounted for")
    oem_main_df = pd.concat([oem_match_df,oem_no_match_df], axis=0)
    print(f"Total Number of OEM Parts accounted for {oem_main_df['searchQuery'].nunique()}")

oem_main_df.to_csv('adhoc_extract/custom_data_extract/oems.csv', index=False, encoding='utf-8')