--By Alejandro Choapa
--the goal of this file is to provide dashboard ready SQL views making it easier on a Databricks dashboard.
--It summarizes performance, showing plots and trend views 

-- flight summary view
CREATE OR REPLACE VIEW flight.flight_summary AS
SELECT 
  flight_id,
  COUNT(*) AS windows, --this is the number of analyzed windows
  AVG(qc_score) AS qc_avg, -- overall quality
  MAX(emi_max) AS emi_peak -- peak EMI level recorded
FROM delta.`/mnt/lake/flight/gold/qc_windowed`
GROUP BY flight_id;

-- now we compare 
CREATE OR REPLACE VIEW flight.emi_vs_jitter AS
SELECT
  w.start AS window_start, flight_id, emi_avg, roll_jitter, pitch_jitter, yaw_jitter
FROM delta.`/mnt/lake/flight/gold/features`; -- gold table generated from silver

--we now implement a trend view across data from flights
CREATE OR REPLACE VIEW flight.qc_trends AS -- hourly trends
SELECT
-- the following line groups by flights, windows, hourly sources
  flight_id, DATE_TRUNC('hour', w.start) AS hour_bucket, AVG(qc_score) AS qc_avg_hour
FROM delta.`/mnt/lake/flight/gold/qc_windowed` -- gold table 
GROUP BY flight_id, DATE_TRUNC('hour', w.start); -- groups on flights/hour to get one row 
