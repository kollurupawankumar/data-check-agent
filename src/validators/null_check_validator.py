from typing import List, Dict, Any
from pyspark.sql import DataFrame
from great_expectations.core import ExpectationConfiguration
from .base_validator import GXBaseValidator
from ..models.validation_result import ValidationResult


class NullCheckValidator(GXBaseValidator):
    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        results = []
        expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": column}
            ) for column in config['columns']
        ]

        try:
            self._create_resources(df, expectations)
            checkpoint = self.context.add_or_update_checkpoint(
                name=self.checkpoint_name,
                validations=[{
                    "batch_request": self.batch_request,
                    "expectation_suite_name": self.suite_name
                }]
            )
            checkpoint_result = checkpoint.run()

            for result in checkpoint_result.list_validation_results():
                for exp_result in result["results"]:
                    # Process results and create ValidationResult objects
                    pass

        finally:
            self._cleanup_resources()

        return results