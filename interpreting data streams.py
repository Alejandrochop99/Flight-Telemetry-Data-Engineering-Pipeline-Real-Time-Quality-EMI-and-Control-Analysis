#code by Alejandro Choapa

# we need to define the data structures of how Spark will read and
#interpret the data stream
# first, we need to import everything from pyspark.sql.types so we can use different data type classsess
from pyspark.sql.types import *

def telemetry_schema():
    #we now need define a function that returns the output for our telemetry data 
    return StructType([
        StructField("timestamp", LongType(), False),   # timestamp
        StructField("seq", LongType(), False), # sequence number to help determine ordering
        StructField("flight_id", StringType(), False), #unique identifier (renamed from drone_id)
        StructField("ax", DoubleType(), True), #acceleration in the x axis
        StructField("ay", DoubleType(), True), #acceleration in the y axis
        StructField("az", DoubleType(), True),  #acceleration in the z axis
        StructField("gx", DoubleType(), True), #gyro reading x axis
        StructField("gy", DoubleType(), True), #gyro reading y axis
        StructField("gz", DoubleType(), True), #gyro reading z axis
        StructField("mx", DoubleType(), True), #magnetometer reading x axis
        StructField("my", DoubleType(), True),#magnetometer reading y axis
        StructField("mz", DoubleType(), True),  #magnetometer reading z axis
        StructField("roll", DoubleType(), True), #roll
        StructField("pitch", DoubleType(), True), #pitch
        StructField("yaw", DoubleType(), True), #yaw
        StructField("cmd_ail", DoubleType(), True), #aileron command
        StructField("cmd_elev", DoubleType(), True),# elevator command
        StructField("cmd_rud", DoubleType(), True), #rudder control
        StructField("rssi", DoubleType(), True), #signal strength
        StructField("emi_noise", DoubleType(), True) # interference reading
    ])
