# Q5
# Να υλοποιηθεί το Query 5 χρησιμοποιώντας τo DataFrame API. Πειραματιστείτε κάνοντας την εισαγωγή των δεδομένων με χρήση αρχείων csv και parquet και σχολιάστε πώς επηρεάζεται η εκτέλεση

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q4").getOrCreate()
df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/yellow_tripdata_2024")
df_zones = spark.read.parquet("hdfs://hdfs-namenode:9000/user/spetroutsatos/data/parquet/taxi_zone_lookup")

zones_pickup = df_zones.alias("pickup")
zones_dropoff = df_zones.alias("dropoff")

df_final = (
    df.join(zones_pickup, col("PULocationID") == col("pickup.LocationID"), how="inner")
      .join(zones_dropoff, col("DOLocationID") == col("dropoff.LocationID"), how="inner")
      .select(
          df["*"],
          col("pickup.Zone").alias("Pickup_Zone"),
          col("dropoff.Zone").alias("Dropoff_Zone")
      )
)

df_final = df_final.filter(df_final["Pickup_Zone"] != df_final["Dropoff_Zone"]).groupBy("Pickup_Zone","Dropoff_Zone").count().orderBy("count", ascending=False)
df_final.show()

df_final.explain(True)

