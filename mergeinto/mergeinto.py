from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# Initialize SparkSession
spark = SparkSession.builder.appName("MergeInto").getOrCreate()

# Define the source and target tables (replace with your actual table names and data loading)
# Assuming you have DataFrames named 'customer_df' (source) and 'dim_customer_df' (target)
# Example data loading (replace with your actual data loading logic):
# customer_df = spark.read.table("prod.db.customer")
# dim_customer_df = spark.read.table("prod.db.dim_customer")

# ---  customers_with_updates CTE equivalent ---
customers_with_updates = (
    customer_df.alias("c")
    .join(
        dim_customer_df.alias("dc"),
        col("c.customer_id")
        == col("dc.customer_id"),  # ON c.customer_id = dc.customer_id
        "inner",
    )
    .where(
        (
            col("c.datetime_updated") > col("dc.datetime_updated")
        )  # WHERE c.datetime_updated > dc.datetime_updated
        & (col("dc.is_current") == True)  # AND dc.is_current = true
    )
    .select("c.*")
)

# --- Union the dataframes for insertion/update ---
# Add a 'join_key' column to customer_df
customer_df = customer_df.withColumn("join_key", col("customer_id"))

# Create a DataFrame from customers_with_updates and add a NULL join_key
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    IntegerType,
)

# Dynamically create schema for null values, handling all customer_df columns
customer_schema = customer_df.schema
customers_with_updates_schema = StructType(
    [StructField("join_key", StringType(), True)] + customer_schema.fields
)
# Create empty rows of data based on this schema, one row for each element in the DF
empty_row = Row(*([None] * len(customers_with_updates_schema)))
empty_rows = spark.createDataFrame(
    [empty_row] * customers_with_updates.count(), customers_with_updates_schema
)
# Drop join_key and then reorder
empty_rows = empty_rows.drop("join_key")
empty_rows = empty_rows.select(customer_df.columns)
# Union all the dataframes together
customers_with_updates = customers_with_updates.union(empty_rows)

# Create a null join_key column
from pyspark.sql.functions import lit

customers_with_updates = customers_with_updates.withColumn(
    "join_key", lit(None).cast("string")
)

# Union the two dataframes
source_df = customer_df.unionByName(customers_with_updates, allowMissingColumns=True)

# --- Perform the merge logic ---
# 1. Update existing records
# Condition: MATCHED AND is_current = true AND s.datetime_updated > t.datetime_updated
# Action: UPDATE SET is_current = false, valid_to = s.datetime_updated
updated_df = (
    dim_customer_df.alias("t")
    .join(source_df.alias("s"), col("t.customer_id") == col("s.join_key"), "inner")
    .where(
        (col("t.is_current") == True)
        & (col("s.datetime_updated") > col("t.datetime_updated"))
    )
    .select(col("t.*"), col("s.datetime_updated").alias("s_datetime_updated"))
)


# Update the is_current and valid_to columns in dim_customer_df
def update_dim_customer(row):
    if (
        row.customer_id
        in updated_df.select("customer_id").rdd.flatMap(lambda x: x).collect()
    ):
        row.is_current = False
        row.valid_to = row.s_datetime_updated
    return row


# Convert the DataFrame to an RDD and update the rows
updated_rdd = dim_customer_df.rdd.map(update_dim_customer)

# Convert the RDD back to a DataFrame
updated_dim_customer_df = spark.createDataFrame(updated_rdd, dim_customer_df.schema)

# 2. Insert new records
# Condition: NOT MATCHED
# Action: INSERT (customer_id,email,first_name,datetime_updated,valid_from,is_current,is_active)
new_customers_df = (
    source_df.alias("s")
    .join(
        dim_customer_df.alias("t"),
        col("s.join_key") == col("t.customer_id"),
        "left_anti",  # equivalent to NOT MATCHED
    )
    .select(
        col("s.customer_id"),
        col("s.email"),
        col("s.first_name"),
        col("s.datetime_updated"),
        col("s.datetime_updated").alias("valid_from"),
        lit(True).alias("is_current"),
        lit(True).alias("is_active"),
    )
)

# Insert the new records into dim_customer_df
final_dim_customer_df = updated_dim_customer_df.unionByName(new_customers_df)

# 3. Update inactive records (Not Matched By Source)
# Condition: NOT MATCHED BY SOURCE
# Action: UPDATE SET is_active = false

# Find which customer_id's are not present in source dataframe.
customer_ids_in_source = source_df.select("customer_id").distinct()

inactive_customers_df = (
    final_dim_customer_df.alias("t")
    .join(
        customer_ids_in_source.alias("s"),
        col("t.customer_id") == col("s.customer_id"),
        "left_anti",
    )
    .select("t.*")
)


# Update the is_active column in dim_customer_df
def update_inactive_customer(row):
    if (
        row.customer_id
        in inactive_customers_df.select("customer_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    ):
        row.is_active = False
    return row


# Convert the DataFrame to an RDD and update the rows
inactive_rdd = final_dim_customer_df.rdd.map(update_inactive_customer)

# Convert the RDD back to a DataFrame
inactive_dim_customer_df = spark.createDataFrame(
    inactive_rdd, final_dim_customer_df.schema
)

# --- Display the result (replace with your actual data writing logic) ---
inactive_dim_customer_df.show()

# Stop the SparkSession
spark.stop()
