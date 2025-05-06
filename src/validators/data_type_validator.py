from typing import List, Dict, Any
from pyspark.sql import DataFrame
from ..models.validation_result import ValidationResult
from .base_validator import BaseValidator
from ..utils.logger import configure_logger

logger = configure_logger(__name__)


class DataTypeValidator(BaseValidator):
    TYPE_MAP = {
        'integer': 'IntegerType',
        'int': 'IntegerType',
        'string': 'StringType',
        'str': 'StringType',
        'double': 'DoubleType',
        'float': 'FloatType',
        'date': 'DateType',
        'timestamp': 'TimestampType',
        'boolean': 'BooleanType'
    }

    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        results = []
        expected_type = config['data_type'].lower()
        expected = self.TYPE_MAP.get(expected_type)

        if not expected:
            return [ValidationResult(
                table_name=table_name,
                check_type="data_type_check",
                check_name="Data type validation",
                status="Fail",
                remarks=f"Unknown data type: {expected_type}"
            )]

        for column in config['columns']:
            try:
                actual_type = str(df.schema[column].dataType)
                status = "Pass" if actual_type == expected else "Fail"
                remarks = "" if status == "Pass" else \
                    f"Expected {expected}, found {actual_type}"

                results.append(ValidationResult(
                    table_name=table_name,
                    check_type="data_type_check",
                    check_name=f"Data type check on {column}",
                    status=status,
                    remarks=remarks
                ))

            except KeyError:
                remarks = f"Column {column} not found in dataframe"
                results.append(ValidationResult(
                    table_name=table_name,
                    check_type="data_type_check",
                    check_name=f"Data type check on {column}",
                    status="Fail",
                    remarks=remarks
                ))

            except Exception as e:
                logger.error(f"Data type check failed for {column}: {str(e)}")
                results.append(ValidationResult(
                    table_name=table_name,
                    check_type="data_type_check",
                    check_name=f"Data type check on {column}",
                    status="Fail",
                    remarks=f"Validation error: {str(e)}"
                ))

        return results