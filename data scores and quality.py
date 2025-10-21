#code by Alejandro Choapa
#the purpose of this code is to convert flight data and turn it into an easy to read report.
#later on this code we will give score, why do we do this?
# we give each window a score to show how reliable the sensor data is,
#we do this instead of separating data into noise, count, etc...

from pyspark.sql import functions as F #importing spark SQL helper functions
from pyspark.sql.window import Window #importingthe API used for row wise operations

SILVER_PATH = "/mnt/lake/flight/silver/telemetry" #this is our string pointing to our silver delta lake storage location

#we must now create a streaming dataframe by reading from the delta table
df = spark.readStream.format("delta").load(SILVER_PATH) #this creates the streaming dataframe

# Windowed stats
win = "100 milliseconds" #we choose a low latency as it provides real time updates that feel live
g = (df
     .withWatermark("ts","2 seconds") #Uses event time ts with a 2 second lateness budget. Any data later than 2s wont update past windows
     .groupBy(F.window("ts", win, win).alias("w"), F.col("flight_id"))
     .agg( #.agg computes window metrics
         #window metrics are data collected about a windows operanting system
         F.count("*").alias("count"),
         # the next line of code will compute the standard dev of the acc and axes of our sensor data 
         F.stddev_pop("ax").alias("ax_std"), F.stddev_pop("ay").alias("ay_std"), F.stddev_pop("az").alias("az_std"), F.stddev_pop("gx").alias("gx_std"), F.stddev_pop("gy").alias("gy_std"), F.stddev_pop("gz").alias("gz_std"),
         # the following line relate to our EMI system
         F.max("emi_noise").alias("emi_max"), F.avg("emi_noise").alias("emi_avg"), F.max("rssi").alias("rssi_max"),)

     .withColumn("qc_missing_ok", (F.col("count") >= 10).cast("int"))  #expected sample/rate completness
     .withColumn("qc_noise_ok", (F.coalesce(F.col("gx_std"),F.lit(0)) < F.lit(80)).cast("int")) # we set out noise cap
     .withColumn("qc_emi_ok", (F.coalesce(F.col("emi_max"),F.lit(0)) < F.lit(0.8)).cast("int")) # 
     .withColumn("qc_score", F.col("qc_missing_ok")*40 + F.col("qc_noise_ok")*30 + F.col("qc_emi_ok")*30) #setting out score out of 100
)

(q1 := g.writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation","/mnt/lake/_checkpoints/qc_windowed")
      .start("/mnt/lake/flight/gold/qc_windowed"))


qc_win = (spark.readStream.format("delta").load(SRC) #this format tails a delta table as a stream 
          #notice we include our SRC files as a windowed QC table
          .select("flight_id", "qc_score", "emi_max", "ax_std", "gx_std", F.col("w.end").cast("timestamp").alias("event_time"))
)

#now we have to communicate with spark how to organize late incoming data
summary = (qc_win
  .withWatermark("event_time", "15 minutes")     # allow late data and enable state cleanup
  .groupBy("flight_id",F.window("event_time", "10 minutes")       # tumbling 10 min windows
           #the previous line allows for correctness, late rows can still update 
  )
  .agg(F.count("*").alias("windows"), # we are now counting how many rows were implemented into the 10 min window
      F.avg("qc_score").alias("qc_avg"), #we provide a score to each counted bucket
      F.max("emi_max").alias("emi_peak"), #this is the peak EMI data observed in 1o min
     #the following two lines calculate both the averages for ac and gyro
      F.avg("ax_std").alias("ax_std_avg"), F.avg("gx_std").alias("gx_std_avg"))
  .select(F.col("window.start").alias("win_start"), F.col("window.end").alias("win_end"),
      "flight_id","windows","qc_avg","emi_peak","ax_std_avg","gx_std_avg"))

q2 = (summary.writeStream
      .format("delta") #this part is important as it writes to delta lake tables
      .outputMode("append")  # communicate with spark to only reject finalized rows                
      .option("checkpointLocation", CHK) # if the begining was important, this part is critical! 
      #this checkpoint stores progress so the data does not have to be re dead
      #ALWAYS KEEP A CHECKPOINT!
      .option("path", DST) #we use  here to write a path only sink to read with spark.read.format
      .toTable("gold.qc_flight_summary_10m") #we use toTable to catalog the table query with SQL
)
