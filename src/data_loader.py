from typing import Tuple, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from .utils.logger import configure_logger
import hashlib

logger = configure_logger(__name__)


class DataLoader:
    """Handles data loading from both tables and SQL queries with source identification"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_data(self, config: Dict[str, Any]) -> Tuple[DataFrame, str, str]:
        """
        Load data from either a table or SQL query
        Returns:
            Tuple[DataFrame, str, str]: (loaded DataFrame, source ID, source type)
        """
        try:
            if 'table_name' in config:
                return self._load_table(config)
            elif 'query' in config:
                return self._load_query(config)
            else:
                raise ValueError("Input must contain either 'table_name' or 'query'")
        except Exception as e:
            logger.error(f"Data loading failed: {str(e)}")
            raise

    def _load_table(self, config: Dict[str, Any]) -> Tuple[DataFrame, str, str]:
        """Load data from a Hive table"""
        table_name = config['table_name']
        logger.info(f"Loading table: {table_name}")
        df = self.spark.read.table(table_name)
        return df, table_name, 'table'

    def _load_query(self, config: Dict[str, Any]) -> Tuple[DataFrame, str, str]:
        """Load data from a SQL query with automatic ID generation"""
        query = config['query']
        query_id = config.get('id', self._generate_query_id(query))
        logger.info(f"Executing query ({query_id}): {query[:50]}...")
        df = self.spark.sql(query)
        return df, query_id, 'query'

    def _generate_query_id(self, query: str) -> str:
        """Generate unique ID from query text"""
        hash_obj = hashlib.sha256(query.encode()).hexdigest()
        return f"query_{hash_obj[:8]}"