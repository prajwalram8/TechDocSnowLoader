# Tech Doc Query & SnowLoader
This is a simple utility created to query the tech-doc backend (OEM - IAM Spare Parts Mapping Service) and load the data to a given table/s in snowflake

This is a rough data loader script created specifically for loading data from techdoc api. A more abstracted reuseable version is in the works.

## Instructions to use
1. Create a virtual environment
   ```python
   pip install virtualenv
   python -m venv <virtualenvname>
   ```
2. Activate virtual environment
   ```bash
   source <virtualenvname>/bin/activate
   ```
3. pip install -r requirements.txt
4. Create a copy of the `default.ini` file in the the config folder and key in the necessary datawarehouse details, log-in info and API key for techdoc
5. From the root folder run the code
   ```bash
   python -m src.main
   ```


