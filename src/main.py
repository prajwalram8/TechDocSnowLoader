import os
from src import project_root
from src.slowder import DataLoader
from src.logger_config import main_logger
from src.techdocpull_mt import extract_data_from_api

CONFIG_FOLDER = os.path.join(project_root,'config')
CONFIG_FILE = os.path.join(CONFIG_FOLDER,'config.ini')


if __name__ == "__main__":
    main_logger.info("Starting application")

    # Initialize Data Load API for Snowflake
    loader = DataLoader(config_path=CONFIG_FILE)

    # Get difference of items between Mapping file and listing document
    sql_query = """
        SELECT DISTINCT 
            "partSKU"
        FROM buyparts24_prod_dwh.raw_mongo.bp24_listings
        WHERE "partSKU" NOT IN (SELECT DISTINCT 
                                    "oem_sku_code"
                                FROM BUYPARTS24_PROD_DWH.ADS.CUST_DM_OEM_AM_MATCHES)
        AND "isDeleted" in (NULL, FALSE)
        AND "hide" in (NULL, FALSE)
    """
    sfqid = loader.execute_query(query=sql_query)

    oem_skus, _ = loader.get_table_result_from_cur(qid=sfqid)

    oem_skus = list(map(lambda x:x[0], oem_skus))

    # If new oems exists enter the work flow
    if len(oem_skus) > 0:
        # extract IAM via API call for the difference skus
        if extract_data_from_api(oem_list=oem_skus, config_path=CONFIG_FILE):
            loader.main_load(name= 'CUST_DATA_OEM_NO_MATCHES_V2', input_location=os.path.join(project_root,'data','no_responses'), staging_location=os.path.join(project_root,'data','upload_stage'))
            loader.main_load(name= 'cust_data_oem_matches_v2', input_location=os.path.join(project_root,'data','oem_matches'), staging_location=os.path.join(project_root,'data','upload_stage'))
        else:
            main_logger.error("Issue with the data extract. Please check logs")
    else:
        main_logger.info("No New OEM SKUs detected")

    gasp_master = True

    if gasp_master:
        loader.main_load(name='cust_data_oem_gasp_matches_v2', input_location=os.path.join(project_root,'data','gasp_master'), staging_location=os.path.join(project_root,'data','upload_stage'))

