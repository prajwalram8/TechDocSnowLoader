import logging

def setup_logging():
    """
    Setup root logger
    """
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        filename='app.log', # log to a file
                        filemode='a') # append to the file, don't overwrite
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
    logging.getLogger('').addHandler(console)


    # 'application' code
    logger = logging.getLogger('main')
    return logger

# Main loogger for the application
main_logger = setup_logging()