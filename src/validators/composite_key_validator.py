import uuid
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from great_expectations.core import ExpectationConfiguration
from .base_validator import GXBaseValidator
from ..models.validation_result import ValidationResult
from ..utils.logger import configure_logger

logger = configure_logger(__name__)


class CompositeKeyValidator(GXBaseValidator):
    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        results = []

        try:
            columns = config['columns']
            expectations = [
                ExpectationConfiguration(
                    expectation_type="expect_compound_columns_to_be_unique",
                    kwargs={"column_list": columns}
                )
            ]

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
                    status = "Pass" if result["success"] else "Fail"
                    remarks = ""

                    if not result["success"]:
                        cols = ", ".join(columns)
                        remarks = f"Duplicate composite keys found in {cols}. Ensure combination uniqueness."

                    results.append(ValidationResult(
                        table_name=table_name,
                        check_type="composite_key",
                        check_name=f"Composite key check on {', '.join(columns)}",
                        status=status,
                        remarks=remarks
                    ))

        except Exception as e:
            logger.error(f"Composite key validation failed: {str(e)}")
            results.append(ValidationResult(
                table_name=table_name,
                check_type="composite_key",
                check_name="Composite key check",
                status="Fail",
                remarks=f"Validation error: {str(e)}"
            ))
        finally:
            self._cleanup_resources()

        return results