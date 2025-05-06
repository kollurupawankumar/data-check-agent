import json
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from .data_loader import DataLoader
from .models.validation_result import ValidationResult
from .validators import (
    NullCheckValidator,
    DataTypeValidator,
    RangeValidator,
    UniqueKeyValidator,
    CompositeKeyValidator,
    CustomQueryValidator
)
from .utils.logger import configure_logger

logger = configure_logger(__name__)


class DataChecker:
    """Main class orchestrating the data quality validation process"""

    def __init__(self, config_path: str, output_path: str):
        self.config_path = config_path
        self.output_path = output_path
        self.spark = SparkSession.builder \
            .appName("DataQualityCheck") \
            .getOrCreate()
        self.validators = self._initialize_validators()

    def _initialize_validators(self) -> Dict[str, Any]:
        """Initialize all available validators"""
        return {
            "null_check": NullCheckValidator(),
            "data_type_check": DataTypeValidator(),
            "range_check": RangeValidator(),
            "unique_key": UniqueKeyValidator(),
            "composite_key": CompositeKeyValidator(),
            "custom_query": CustomQueryValidator(self.spark)
        }

    def _load_config(self) -> List[Dict[str, Any]]:
        """Load JSON configuration file"""
        try:
            with open(self.config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Config load failed: {str(e)}")
            raise

    def _save_results(self, results: List[ValidationResult]):
        """Save validation results to CSV"""
        if not results:
            logger.warning("No validation results to save")
            return

        try:
            # Convert ValidationResult objects to dictionary format
            results_dict = [result.to_dict() for result in results]

            df = self.spark.createDataFrame(results_dict)
            df.write.csv(self.output_path, header=True, mode="overwrite")
            logger.info(f"Results saved to {self.output_path}")
        except Exception as e:
            logger.error(f"Failed to save results: {str(e)}")
            raise

    def _process_entry(self, entry: Dict[str, Any]) -> List[ValidationResult]:
        """Process a single configuration entry"""
        results = []
        loader = DataLoader(self.spark)

        try:
            # Load data and get source metadata
            df, source_id, source_type = loader.load_data(entry)

            # Process all validations for this entry
            for validation in entry.get("validations", []):
                validator = self.validators.get(validation["type"])
                if not validator:
                    msg = f"No validator found for type: {validation['type']}"
                    logger.warning(msg)
                    results.append(ValidationResult(
                        source_id=source_id,
                        source_type=source_type,
                        check_type=validation["type"],
                        check_name="Validation setup",
                        status="Fail",
                        remarks=msg
                    ))
                    continue

                try:
                    # Execute validation and collect results
                    validation_results = validator.process(
                        df=df,
                        config=validation,
                        source_id=source_id,
                        source_type=source_type
                    )
                    results.extend(validation_results)
                except Exception as e:
                    logger.error(f"Validation failed: {str(e)}")
                    results.append(ValidationResult(
                        source_id=source_id,
                        source_type=source_type,
                        check_type=validation["type"],
                        check_name="Validation execution",
                        status="Fail",
                        remarks=f"Validation error: {str(e)}"
                    ))

        except Exception as e:
            logger.error(f"Entry processing failed: {str(e)}")
            results.append(ValidationResult(
                source_id=entry.get("table_name", "unknown"),
                source_type="unknown",
                check_type="System",
                check_name="Data loading",
                status="Fail",
                remarks=f"Processing error: {str(e)}"
            ))

        return results

    def run(self):
        """Main execution method"""
        try:
            # Load configuration and process all entries
            config = self._load_config()
            all_results = []

            logger.info(f"Starting data quality check with {len(config)} entries")
            for entry in config:
                all_results.extend(self._process_entry(entry))

            # Save final results
            self._save_results(all_results)
            logger.info("Data quality check completed successfully")

        except Exception as e:
            logger.critical(f"Fatal error in data checker: {str(e)}")
            raise
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")