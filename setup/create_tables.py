from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


spark.sql("DROP TABLE IF EXISTS prod.db.customer")


# Table DDL for customer upstream table
spark.sql("""
CREATE TABLE IF NOT EXISTS prod.db.customer (
    customer_id INT,
    email STRING,
    first_name STRING,
    datetime_updated TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2'
)""")

# Insert some fake data for the OLTP tables
spark.sql("""
-- Insert sample customers
INSERT INTO prod.db.customer VALUES
  (1, 'john.doe@example.com', 'John', TIMESTAMP '2023-01-15 08:30:00'),
  (2, 'jane.smith@example.com', 'Jane', TIMESTAMP '2023-03-18 09:10:30'),
  (3, 'robert.brown@example.com', 'Robert', TIMESTAMP '2023-02-10 11:05:45');
""")

spark.sql("DROP TABLE IF EXISTS prod.db.dim_customer")

spark.sql("""
CREATE TABLE IF NOT EXISTS prod.db.dim_customer (
    customer_id INT,
    email STRING,
    first_name STRING,
    datetime_updated TIMESTAMP,
    -- scd2 columns
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    is_active BOOLEAN
) USING iceberg
PARTITIONED BY (datetime_updated)
TBLPROPERTIES (
    'format-version' = '2'
);""")
