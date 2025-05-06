# Data Quality Checker

A PySpark-based data validation framework using Great Expectations.

## Features

- **Multiple Validation Types**: 
  - Null checks
  - Unique key validation
  - Composite key validation
  - Data type checks
  - Range validation
  - Custom SQL query validation

- **Flexible Input Sources**:
  - Direct table access
  - Custom SQL queries
  - Automatic query identification

- **Comprehensive Reporting**:
  - CSV output with detailed results
  - Source identification (table/query)
  - Actionable remediation advice
  - Error categorization

## Installation

1. **Requirements**:
   - Python 3.8+
   - Spark
   ```bash
   pip install -r requirements.txt