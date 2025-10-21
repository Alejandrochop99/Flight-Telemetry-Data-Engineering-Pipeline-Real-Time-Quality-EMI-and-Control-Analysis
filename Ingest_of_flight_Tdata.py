# Databricks notebook source
#The purpose of this file is to ingests continuous flight telemetry data to then organize it in Bronze and silver layers
#We need to ensure ensures that all inflight incoming data is available for further analytics and visualization
#Bronze layer: stores raw unprocessed data as it comes from the source in this case our sensor.
#Silver layer: Stores a refined and validated data.

#now we'll bring spark functions to transform columns into streaming pipelines
#streaming pipelines collect, processe and deliver data as it is generated.
#We must provide data types and struck builders
#then we need a Sparksession to read our data from the entry point to spark.
from pyspark.sql import functions as F #transforming data to frames and pipelines
from pyspark.sql import types as T #provides data types
from pyspark.sql import SparkSession #reading the data
from scripts.schema import telemetry_schema #describes the layout
#Using an explicit schema is faster and safer than letting Spark infer types


#creating the spark session
spark = SparkSession.builder.getOrCreate()

#we now need to read the streamining sources which is provided by our telemetry 
#we also need to determines which ingestion source to use and checkpoint directories so the stream can resume if interrupted.
SOURCE = dbutils.widgets.getArgument("SOURCE", "autoloader") #reads the databricks names
BRONZE_PATH = "/mnt/lake/flight/bronze/telemetry" # raw telementry
SILVER_PATH = "/mnt/lake/flight/silver/telemetry" # cleaned telemetry
#we must now add checkpoints as structures streaming must guarantee checkpoints
CHECKPOINT_BRONZE = "/mnt/lake/_checkpoints/telemetry_bronze"
CHECKPOINT_SILVER = "/mnt/lake/_checkpoints/telemetry_silver"

#NOw we need to return a standardized streaming dataframe witha string column
# a standardized streaming dataframe is a kafka topic projected onto an infinate table
# a kafka topic is a named long set of events organized into categories, storing and distributing data
def read_stream_source(): # returning a dataframe
    if SOURCE == "eventhubs": #reads telemetry from an Azure Event Hub using a secret connection string
        # a secret string is simply a connection string containing sensitive information
        connection_string = dbutils.secrets.get("flight-scope","eventhubs-conn")
        eventhubs_conf = {
            "eventhubs.connectionString": connection_string
        }
        return (spark.readStream.format("eventhubs").options(**eventhubs_conf).load()
                .select(F.col("body").cast("string").alias("raw")))
    elif SOURCE == "kafka": # subscribes to a topic using a bootstrap server address from azure
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", dbutils.secrets.get("flight-scope","kafka-bootstrap"))
                .option("subscribe", "flight-telemetry") #subscribing to the flight telemetry topic
                .load()
                #storing payload in value to raw.
                .select(F.col("value").cast("string").alias("raw")))
    else:
        #we'll now use Autoloader to watch the folder and incrementally ingest new files and reads CSV
        return (spark.readStream.format("cloudFiles") #.format brings Databricks Auto Loader
                .option("cloudFiles.format","csv") #we expect a csv file type, we can change to json if needed
                .option("header","true") #ONLY first row contains column names!!! IMPORTANT!
                .load("/mnt/landing/flight/telemetry") #this is the directory
                .selectExpr("to_json(named_struct(*)) as raw")) #converts to JSON string

raw_stream = read_stream_source() #calling the previous function to retunr a dataframe

#
parsed = (raw_stream #transformating pipeline on that streaming Dataframe.
          .select(F.col("raw"), #keeps the original column
                  F.from_json(F.col("raw"), telemetry_schema()).alias("js")) #this line checks if you defined the function correctly
          .select("js.*") #expands the struct
          .withColumn("ingest_ts", F.current_timestamp()) #we now add a ingestion timestamp
          .withColumn("event_ts", (F.col("timestamp_ms")/1000).cast("timestamp"))#converting "timestamp" to a proper type
         )

# Write BRONZE
(bronze_query := parsed.writeStream #we are now starting a streaming sink from the parsed dataframe
    # a streaming sink is the destination or endpoint for data that has been processed
    .format("delta") #writing the streams as a delta lake files
    .option("checkpointLocation", CHECKPOINT_BRONZE) #storing 
    .outputMode("append") #only new rows written
    .start(BRONZE_PATH)) #starts a new stream writer



# Clean & normalize to SILVER
bronze = (spark.readStream.format("delta").load(BRONZE_PATH)) # we're now running a bronze to silver pipeline

silver = (bronze
    .withColumn("ts", F.to_timestamp((F.col("timestamp")/1000).cast("double")))
    #we now compute sequence gaps
    .withColumn("seq_gap", F.col("seq") - F.lag("seq").over(Window.partitionBy("flight_id").orderBy("timestamp")))
    .withColumn("source", F.lit(SOURCE))
    Window.partitionBy("id").orderBy("ts")

)

(silver_query := silver.writeStream # starting a streaming sink 
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .outputMode("append")
    .start(SILVER_PATH))
