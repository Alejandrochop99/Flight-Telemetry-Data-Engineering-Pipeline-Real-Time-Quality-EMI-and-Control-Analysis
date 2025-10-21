# by Alejandro Choapa

tables = [
  "/mnt/lake/flight/bronze/telemetry",
  "/mnt/lake/flight/silver/telemetry",
  "/mnt/lake/flight/gold/qc_windowed",
  "/mnt/lake/flight/gold/qc_flight_summary",
  "/mnt/lake/flight/gold/features"
]

for t in tables:
    spark.sql(f"OPTIMIZE delta.`{t}` ZORDER BY (flight_id)")
