import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logging(name):
    """
    Setup logger for a specific module with log rotation.
    """
    logger = logging.getLogger(name)
    
    if not logger.hasHandlers():
        # Define the log format
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        
        # Define the TimedRotatingFileHandler
        file_handler = TimedRotatingFileHandler('app.log', when='D', interval=1, backupCount=6)
        file_handler.setFormatter(log_format)
        file_handler.setLevel(logging.INFO)
        
        # Define the console handler
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
        
        # Add handlers to the root logger
        root_logger = logging.getLogger('')
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console)
    
    # Specific adjustments for Snowflake connector logs
    if name.startswith('snowflake.connector'):
        logger.setLevel(logging.WARNING)
    
    return logger