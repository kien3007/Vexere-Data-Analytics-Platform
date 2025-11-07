from pyspark.sql import SparkSession
import os

def get_spark_session(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")

        # Memory tuning
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.shuffle.partitions", "24")

        # ---------------- S3 / MinIO (Spark S3A) ----------------
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "64")
        .config("spark.hadoop.fs.s3a.multipart.size", "64M")

        # ---------------- ICEBERG CATALOG (DÙNG HIVE) ----------------
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"))
        .config("spark.sql.catalog.lakehouse.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3://datalake"))
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")

        # Ghi đè partition kiểu động khi insert overwrite
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # ---------------- Không dùng Hive Metastore ----------------
        .config("spark.sql.catalogImplementation", "in-memory")

        .getOrCreate()
    )