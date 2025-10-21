#code by Alejandro Choapa
#the goal of this file is to Derive performance features from the silver telemetry stream.

# import spark SQL helpers
from pyspark.sql import functions as F # importing as F
from pyspark.sql.window import Window

#clean silver telemetry delta table lives

SILVER_PATH = "/mnt/lake/flight/silver/telemetry" # start a streaming read from the silver Delta path

#start structured streaming from silver table inot df
df = spark.readStream.format("delta").load(SILVER_PATH) 

# now we approximate error by local variance of roll, pitch and yaw
win = "100 milliseconds"
features = (df
    .withWatermark("ts","2 seconds") #allows eevnts to be up to 2 seconds late
    .groupBy(F.window("ts",win,win).alias("w"), "flight_id") #grouping into tumbling windows 
    # now the next few lines will provide the standard deviation of rool, pitch, yaw, aileron, elevtor, and rudder 
    .agg(F.stddev_pop("roll").alias("roll_jitter"), F.stddev_pop("pitch").alias("pitch_jitter"), F.stddev_pop("yaw").alias("yaw_jitter"), F.stddev_pop("cmd_ail").alias("ail_jitter"),
        F.stddev_pop("cmd_elev").alias("elev_jitter"), F.stddev_pop("cmd_rud").alias("rud_jitter"), F.max("emi_noise").alias("emi_max"),F.avg("emi_noise").alias("emi_avg"))
   #we now have the maximun level and the average level of EMI in the window
    .withColumn("perf_anomaly", (F.col("roll_jitter")>2.5) | (F.col("pitch_jitter")>2.5) | (F.col("yaw_jitter")>2.5))
    .withColumn("emi_spike", F.col("emi_max") > 0.8) #flags anything over 0.8
)

(fq := features.writeStream
      .format("delta")  # write as delta streams
      .outputMode("append") #append each window as it completees
      .option("checkpointLocation","/mnt/lake/_checkpoints/features") # checkpoint
      .start("/mnt/lake/flight/gold/features")) #sink for gold features

# for streaming first environments we'll build a separate job to refresh correlations
feat = spark.read.format("delta").load("/mnt/lake/flight/gold/features")
corr = (feat
    .groupBy("flight_id") #computing correlations
    #the next few lines will computer correlation with avg EMI, roll, pitch, and ay jitter
    .agg(F.corr("emi_avg","roll_jitter").alias("corr_emi_roll"), F.corr("emi_avg","pitch_jitter").alias("corr_emi_pitch"), F.corr("emi_avg","yaw_jitter").alias("corr_emi_yaw"),
        F.avg(F.when(F.col('emi_spike') & F.col('perf_anomaly'), 1).otherwise(0)).alias("p(anomaly|emi)"))
)

corr.createOrReplaceTempView("emi_perf_corr") # we need this in order to SQL this later
# refer to SQL below references global_temp.emi_perf_corr
