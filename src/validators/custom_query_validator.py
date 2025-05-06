from typing import List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from ..models.validation_result import ValidationResult
from ..utils.logger import configure_logger

logger = configure_logger(__name__)


class CustomQueryValidator:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        try:
            query = config['query']
            expected = config['expected_value']

            result_df = self.spark.sql(query)
            actual = result_df.collect()[0][0] if result_df.count() > 0 else None

            status = "Pass" if actual == expected else "Fail"
            remarks = ""

            if status == "Fail":
                remarks = f"Query returned {actual}, expected {expected}. Investigate data anomalies."

            return [ValidationResult(
                table_name=table_name,
                check_type="custom_query",
                check_name="Custom query validation",
                status=status,
                remarks=remarks
            )]

        except Exception as e:
            logger.error(f"Custom query validation failed: {str(e)}")
            return [ValidationResult(
                table_name=table_name,
                check_type="custom_query",
                check_name="Custom query",
                status="Fail",
                remarks=f"Validation error: {str(e)}"
            )]