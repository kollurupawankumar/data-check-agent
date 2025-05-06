import uuid
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import List, Dict, Any
from great_expectations.core import ExpectationConfiguration
from src.models.validation_result import ValidationResult


class BaseValidator(ABC):
    @abstractmethod
    def process(self, df: DataFrame, config: Dict[str, Any], table_name: str) -> List[ValidationResult]:
        pass


class GXBaseValidator(BaseValidator):
    def __init__(self):
        self.context = gx.get_context()

    def _create_resources(self, df: DataFrame, expectations: List[ExpectationConfiguration]):
        self.suite_name = f"temp_suite_{uuid.uuid4().hex}"
        self.datasource_name = f"datasource_{uuid.uuid4().hex}"
        self.checkpoint_name = f"checkpoint_{uuid.uuid4().hex}"

        datasource = self.context.sources.add_spark(self.datasource_name)
        data_asset = datasource.add_dataframe_asset(f"data_asset_{uuid.uuid4().hex}", dataframe=df)
        self.batch_request = data_asset.build_batch_request()

        suite = self.context.add_expectation_suite(self.suite_name)
        for exp in expectations:
            suite.add_expectation(exp)
        self.context.save_expectation_suite(suite)

    def _cleanup_resources(self):
        self.context.delete_expectation_suite(self.suite_name)
        self.context.delete_checkpoint(self.checkpoint_name)
        self.context.delete_datasource(self.datasource_name)