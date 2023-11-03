from pyspark.sql import SparkSession
from pyspark.sql.functions import when


def add(a, b):
    return a + b


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("example").getOrCreate()

    # Load the dataset
    data = spark.read.csv("./data.csv", header=True, inferSchema=True)

    # Calculate the average salary using Spark SQL
    data.createOrReplaceTempView("alcohol")
    data_value = spark.sql(
        "SELECT AVG(Data_value) AS AvgValue FROM alcohol WHERE Series_reference = 'ALCA.SAABS'"
    )
    data_value.show()

    # Perform the data transformation
    data = data.withColumn(
        "Value_Group", when(data.Data_value < 100, "Less").otherwise("More")
    )

    # Show the transformed DataFrame
    data.show()

    # Stop the SparkSession
    spark.stop()
