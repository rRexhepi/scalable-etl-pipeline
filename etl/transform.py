import subprocess
import sys
import os
import logging
from datetime import datetime

def setup_logger(log_level=logging.INFO):
    """
    Sets up the logger for the transformation orchestrator script.
    Logs are output to both stdout and a log file.
    """
    logger = logging.getLogger("TransformOrchestratorLogger")
    logger.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Stream Handler (stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # File Handler
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'transform_orchestrator.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def main():
    """
    Orchestrates the execution of the Spark transformation script.
    """
    logger = setup_logger()
    logger.info("Starting Transformation Orchestrator.")
    
    try:
        # Define the path to the Spark transformation script
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        transform_script = os.path.join(project_root, 'scripts', 'transform.py')
        
        if not os.path.exists(transform_script):
            logger.error(f"Transformation script not found at {transform_script}")
            sys.exit(1)
        
        # Execute the Spark transformation script
        logger.info(f"Executing Spark Transformation Script: {transform_script}")
        result = subprocess.run(
            [sys.executable, transform_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        # Log the output of the transformation script
        logger.info("Transformation Script Output:")
        logger.info(result.stdout)
        
        logger.info("Transformation Orchestrator Completed Successfully.")
    
    except subprocess.CalledProcessError as e:
        logger.error("Transformation Orchestrator Failed.")
        logger.error(f"Error Output:\n{e.stderr}")
        sys.exit(1)
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()