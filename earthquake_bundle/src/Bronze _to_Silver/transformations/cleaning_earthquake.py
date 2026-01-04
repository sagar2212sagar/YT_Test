import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
catalog_name=spark.conf.get("catalog_name")
volumne_path = f"/Volumes/{catalog_name}/bronze/earthquake/*.json"

# Define schema as a StructType object
properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("title", StringType()),
    ]
)

geometry_schema = StructType([StructField("coordinates", ArrayType(DoubleType()))])

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

schema = ArrayType(feature_schema)


@dlt.view(name="earthquake_view")
def earthquake_view():
    df = (
        spark.readStream.format("cloudfiles")
        .option("cloudfiles.format", "json")
        .load(volumne_path)
        .withColumn("_load_timestamp", F.current_timestamp())
    )
    # Parse the features column from STRING to ARRAY<STRUCT>
    df_parsed = df.withColumn("parsed", F.from_json(F.col("features"), schema))
    # Explode the parsed array
    df_exploded = df_parsed.select(
        F.explode("parsed").alias("feature"), "_load_timestamp"
    )
    # Select nested fields
    df2 = df_exploded.select(
        "feature.id",
        "feature.properties.*",
        "feature.geometry.coordinates",
        "_load_timestamp",
    )
    df2 = (
        df2.withColumn("time", F.from_unixtime(F.col("time") / 1000).cast("timestamp"))
        .withColumn("longitude", df2["coordinates"][0])
        .withColumn("latitude", df2["coordinates"][1])
        .withColumn("depth", df2["coordinates"][2])
        .withColumn("mag",df2["mag"].cast(DoubleType()))
        .withColumn("sig",df2["sig"].cast(DoubleType()))
        .withColumn("nst",df2["nst"].cast(DoubleType()))
        .withColumn("dmin",df2["dmin"].cast(DoubleType()))
        .withColumn("rms",df2["rms"].cast(DoubleType()))
    )

    return df2


dlt.create_streaming_table(
    name="earthquake_data",
    comment="SCD1 Silver target table",
)

dlt.apply_changes(
    target="earthquake_data",
    source="earthquake_view",
    keys=["id"],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1",
)
