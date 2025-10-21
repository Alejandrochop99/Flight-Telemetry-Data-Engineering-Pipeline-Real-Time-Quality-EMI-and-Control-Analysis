--by Alejandro Choapa
--databricks table

--first, let's create a database that once it already exisis, nothing happens
CREATE DATABASE IF NOT EXISTS flight;

--now let's creata a managed reference
CREATE TABLE IF NOT EXISTS flight.qc_windowed -- this means it is safe to run multiple times 
USING DELTA -- we tell databricks we wish for a delta table
LOCATION '/mnt/lake/flight/gold/qc_windowed'; -- storage path

-- the following lines will be the same idea as before, just that this time if is for a
-- gold summary table
CREATE TABLE IF NOT EXISTS flight.qc_flight_summary
USING DELTA
LOCATION '/mnt/lake/flight/gold/qc_flight_summary';

--resister wished features, EMI and metrics for the SQL dashboard
CREATE TABLE IF NOT EXISTS flight.features
USING DELTA
LOCATION '/mnt/lake/flight/gold/features';
