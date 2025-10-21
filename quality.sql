-- By Alejandro Choapa
-- checks for flight telemetry data. Then it adds simple true/flase flags.
--It also converst the quality expectations into real SQL checks.

--we are now going to make a temp view we can reuse later
CREATE OR REPLACE TEMP VIEW qc_rules AS
SELECT -- builds qualitry metrics
  window(ts, '100 milliseconds') AS w, drone_id, -- groups rows into 100 ms windows
  COUNT(*) AS sample_count, -- this counts how many rows are ina  window
  SUM(CASE WHEN ax IS NULL OR ay IS NULL OR az IS NULL THEN 1 ELSE 0 END) AS imu_nulls, -- count missing accelerometer values
  SUM(CASE WHEN gx IS NULL OR gy IS NULL OR gz IS NULL THEN 1 ELSE 0 END) AS gyro_nulls, -- count missing gyro values
  STDDEV_POP(gx) AS gx_std, -- stand dev for gyro x, we use this as a noise proxy
  MAX(emi_noise) AS emi_max -- max level of EMI seen in the window
FROM delta.`/mnt/lake/flight/silver/telemetry`  -- this reads from silver table
GROUP BY window(ts, '100 milliseconds'), drone_id; -- finalizing grouping

-- example 
SELECT
  w, drone_id, sample_count, imu_nulls, gyro_nulls, gx_std, emi_max, -- this line keeps raw metrics from debugging
-- true or flase statements
(sample_count >= 10) AS ok_min_samples, (imu_nulls = 0 AND gyro_nulls = 0) AS ok_no_nulls, (gx_std < 80) AS ok_noise, (emi_max < 0.8) AS ok_emi
FROM qc_rules; -- running checks
