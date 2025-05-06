import uuid
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from great_expectations.core import ExpectationConfiguration
from src.validators.base_validator import GXBaseValidator
from src.models.validation_result import ValidationResult
from src.utils.logger import configure_logger

logger = configure_logger(__name__)


class RangeValidator(GXBaseValidator):
    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        results = []
        expectations = []

        try:
            min_val = config['min']
            max_val = config['max']

            for column in config['columns']:
                expectations.append(ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": column,
                        "min_value": min_val,
                        "max_value": max_val
                    }
                ))

            self._create_resources(df, expectations)
            checkpoint = self.context.add_or_update_checkpoint(
                name=self.checkpoint_name,
                validations=[{
                    "batch_request": self.batch_request,
                    "expectation_suite_name": self.suite_name
                }]
            )

            checkpoint_result = checkpoint.run()

            for validation_result in checkpoint_result.list_validation_results():
                for result in validation_result["results"]:
                    column = result["expectation_config"]["kwargs"]["column"]
                    status = "Pass" if result["success"] else "Fail"
                    remarks = ""

                    if not result["success"]:
                        remarks = (f"Values in {column} outside range [{min_val}, {max_val}]. "
                                   "Check data sources for anomalies.")

                    results.append(ValidationResult(
                        table_name=table_name,
                        check_type="range_check",
                        check_name=f"Range check on {column}",
                        status=status,
                        remarks=remarks
                    ))

        except Exception as e:
            logger.error(f"Range validation failed: {str(e)}")
            results.append(ValidationResult(
                table_name=table_name,
                check_type="range_check",
                check_name="Range check",
                status="Fail",
                remarks=f"Validation error: {str(e)}"
            ))
        finally:
            self._cleanup_resources()

        return results