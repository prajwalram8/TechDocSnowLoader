import os
import logging
import requests
import pandas as pd
from tqdm import tqdm
from src import project_root
from configparser import ConfigParser
from datetime import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
data_stage_location= os.path.join(project_root, 'data')

def extract_data_from_api(oem_list: list, config_path: str, batch_size=5000) -> None:
    config = ConfigParser()
    logger.info(f"Config Path: {config_path}")
    config.read(config_path)

    URL = 'https://webservice.tecalliance.services/pegasus-3-0/services/TecdocToCatDLB.jsonEndpoint'
    params = {'api_key': config['techdoc']['api_key']}
    payload = create_payload()

    for start in range(0, len(oem_list), batch_size):
        end = start + batch_size
        logger.info(f"Dispatching elements between index {start} to {end}")
        batch = oem_list[start:end]
        art, no_resp, prob = process_batch(batch, URL, params, payload)
        save_data_in_batches(art, no_resp, prob, end)
        
    logger.info("Extraction completed successfully")
    return True

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
            "includeGenericArticles": 'true',
            "includeOEMNumbers": 'false'
        }
    }

def process_batch(batch, URL, params, payload):
    articles, no_response_list, problem_items = [], [], []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_sku = {executor.submit(process_oem_sku, URL, params, payload, sku): sku for sku in batch}
        for future in as_completed(future_to_sku):
            art, no_resp, prob = future.result()
            articles.extend(art)
            no_response_list.extend(no_resp)
            problem_items.extend(prob)
    return articles, no_response_list, problem_items


def process_oem_sku(URL, params, payload, oem_sku):
    with requests.Session() as session:
        articles, no_response_list, problem_items = [], [], []
        page = 1
        while True:
            payload['getArticles']['searchQuery'] = oem_sku
            payload['getArticles']['page'] = page
            try:
                response = session.post(url=URL, params=params, json=payload)
                if response.status_code == 200:
                    status = handle_response(response, oem_sku, articles, no_response_list, page)
                    if status:
                        page += 1
                    else:
                        break
                else:
                    logger.error(f"Error {response.status_code}: {response.text}")
                    if page == 1:
                        problem_items.append({'OEM SKU': oem_sku, 'Error': response.text})
                    break
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                break
        return articles, no_response_list, problem_items

def handle_response(response, oem_sku, articles, no_response_list, page):
    response_json = response.json()
    articles_data = response_json.get('articles', [])
    if articles_data:
        df_articles = pd.concat([
            pd.json_normalize(articles_data, 'searchQueryMatches', ['dataSupplierId', 'articleNumber', 'mfrName'], 'part_'),
            pd.json_normalize(articles_data, 'genericArticles')
        ], axis=1)
        df_articles['OEM SKU'] = oem_sku
        articles.append(df_articles)
        return True
    elif page == 1:
        no_response_list.append({'OEM SKU': oem_sku})
        return False
    
def save_data_in_batches(articles, no_response_list, problem_items, index):
    try:
        if articles:
            save_to_csv(pd.concat(articles).reset_index(drop=True), "oem_matches", index)
        if no_response_list:
            save_to_csv(pd.DataFrame(no_response_list), "no_responses", index)
        if problem_items:
            save_to_csv(pd.DataFrame(problem_items), "errors", index)
    except Exception as e:
        logger.warning(f"Exception occurred during saving CSV: {e}")

def save_to_csv(df, file_type, index):
    start, end = max(0, index - 5000), index
    dt_stamp = dt.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(data_stage_location,file_type,f"{file_type}_{start}_{end}_{dt_stamp}.csv")
    df.to_csv(file_path, index=False)
