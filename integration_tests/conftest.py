"""Configure Spark session for dbt integration tests with Delta Lake support."""
import os
from pyspark.sql import SparkSession


def pytest_configure(config):
    """Configure Spark session with Delta Lake."""
    # Set up environment variable for dbt-spark to use existing session
    os.environ['DBT_SPARK_PYTEST'] = 'true'
    
    # Create or get Spark session with Delta Lake configuration
    spark = (SparkSession.builder
        .appName("dbt-tbmacro-integration-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .master("local[*]")
        .getOrCreate())
    
    # Store in global namespace for dbt-spark to access
    import sys
    sys.modules['__main__'].spark = spark
