from sqlalchemy import create_engine
import yaml
import os

def get_db_engine(config_path='config/config.yaml'):
    """
    Creates and returns a SQLAlchemy engine for connecting to the PostgreSQL database.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        sqlalchemy.Engine: SQLAlchemy engine instance connected to the specified PostgreSQL database.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    db_config = config.get('database')
    if not db_config:
        raise KeyError("Database configuration not found in the config file.")
    
    required_keys = ['host', 'port', 'user', 'password', 'dbname']
    for key in required_keys:
        if key not in db_config:
            raise KeyError(f"Database configuration missing '{key}' key.")
    
    engine_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    try:
        engine = create_engine(engine_url)
    except Exception as e:
        raise ConnectionError(f"Failed to create engine: {e}")
    
    return engine