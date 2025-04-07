import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def extract(spark, source_table):
    """
    Extract data from the source table.
    """
    df = spark.table(source_table)
    return df


def transform(df, dim_table):
    """
    Transform the data by merging updates and inserting new records.
    """
    dim_df = spark.table(dim_table)

    # Identify customers with updates
    customers_with_updates = (
        df.alias("c")
        .join(dim_df.alias("dc"), df["customer_id"] == dim_df["customer_id"], "inner")
        .where(
            (df["datetime_updated"] > dim_df["datetime_updated"])
            & (dim_df["is_current"] == True)
        )
        .select("c.*")
    )

    # Union new customers and updated customers
    new_and_updated_customers = df.selectExpr("customer_id as join_key", "*").unionAll(
        customers_with_updates.selectExpr("NULL as join_key", "*")
    )

    return new_and_updated_customers, dim_df


def load(spark, transformed_data, target_table):
    """
    Load the transformed data into the target table.
    """
    new_and_updated_customers, dim_df = transformed_data
    target_df = dim_df

    # Update existing customers
    updated_customers = (
        target_df.alias("t")
        .join(
            new_and_updated_customers.alias("s"),
            target_df["customer_id"] == new_and_updated_customers["join_key"],
            "inner",
        )
        .where(
            (target_df["is_current"] == True)
            & (
                new_and_updated_customers["datetime_updated"]
                > target_df["datetime_updated"]
            )
        )
        .select("t.*", "s.datetime_updated")
    )

    # Set is_current to false for updated customers
    target_df = (
        target_df.alias("t")
        .join(
            updated_customers.select("customer_id").distinct().alias("u"),
            target_df["customer_id"] == updated_customers["customer_id"],
            "left_anti",
        )
        .unionAll(
            updated_customers.withColumn("is_current", lit(False)).withColumnRenamed(
                "datetime_updated", "valid_to"
            )
        )
    )

    # Insert new customers
    new_customers = (
        new_and_updated_customers.alias("s")
        .join(
            target_df.select("customer_id").distinct().alias("t"),
            new_and_updated_customers["customer_id"] == target_df["customer_id"],
            "left_anti",
        )
        .select("s.*")
    )

    # Insert new customers into the target table
    target_df = target_df.unionAll(
        new_customers.withColumn("valid_from", new_customers["datetime_updated"])
        .withColumn("is_current", lit(True))
        .withColumn("is_active", lit(True))
    )

    # Write to target table
    target_df.write.mode("overwrite").saveAsTable(target_table)


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("MergeInto").getOrCreate()

    # Define source and target tables
    source_table = "prod.db.customer"
    dim_table = "prod.db.dim_customer"
    target_table = "prod.db.dim_customer"

    # Extract
    customer_df = extract(spark, source_table)

    # Transform
    transformed_data = transform(customer_df, dim_table)

    # Load
    load(spark, transformed_data, target_table)

    # Stop Spark session
    spark.stop()
