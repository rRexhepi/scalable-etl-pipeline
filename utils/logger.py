import logging
import os
import yaml

def setup_logger(config_path='config/config.yaml'):
    """
    Sets up the logger based on configurations.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)

    log_config = config.get('logging', {})
    log_level = log_config.get('level', 'INFO')
    log_file = log_config.get('file', 'logs/etl.log')

    # Ensure log directory exists
    log_dir = os.path.dirname(log_file)
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    return logger