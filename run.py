import sys
import logging
from src.data_checker import DataChecker

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run.py <config_path> <output_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    output_path = sys.argv[2]

    checker = DataChecker(config_path, output_path)
    try:
        checker.run()
    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        sys.exit(1)