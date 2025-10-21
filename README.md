# Flight Telemetry Data Engineering Pipeline
### Real-Time Flight Analytics, EMI Monitoring, and Control Stability Scoring
**Author:** Alejandro Choapa  
**Technologies:** PySpark, Delta Lake, Databricks, SQL, Azure Event Hubs, Kafka, JSONC  

[![Databricks](https://img.shields.io/badge/Platform-Databricks-orange)]()
[![PySpark](https://img.shields.io/badge/Framework-PySpark-blue)]()
[![Azure](https://img.shields.io/badge/Cloud-Azure-lightblue)]()
[![License](https://img.shields.io/badge/License-MIT-green)]()

---

## Overview

This project implements a real-time flight telemetry ingestion and analytics system built on Databricks Delta Lake architecture (Bronze → Silver → Gold).  
It continuously ingests flight sensor data (IMU, gyroscope, magnetometer, and EMI readings) and derives quality metrics, control-surface jitter features, and performance correlations, all in streaming mode.

The pipeline was designed as the cloud and analytics counterpart to my C++ Flight Stabilization Firmware for Arduino-Based RC Aircraft, which controls five servos and monitors electromagnetic interference (EMI) in flight.  
While the firmware collects and transmits real-time telemetry, this pipeline performs analytical evaluation—measuring data quality, EMI impact, and control performance.

---

## Architecture Overview

```
C++ Flight Firmware (5-Servo Stabilizer)
    → Sends IMU, Control, and EMI Telemetry
                ↓
Databricks Streaming Ingestion (Event Hubs / Kafka / Autoloader)
                ↓
Bronze Layer: Raw Flight Telemetry (CSV/JSON ingest)
                ↓
Silver Layer: Cleaned & Normalized Stream (timestamped, typed, validated)
                ↓
Gold Layer: Feature & Quality Tables
    - qc_windowed (100 ms QC)
    - qc_flight_summary (10 min summaries)
    - features (EMI and Control Jitter)
                ↓
SQL Views & Dashboards
    - flight_summary
    - emi_vs_jitter
    - qc_trends
```

---

## Core Components

### 1. Ingest_of_flight_Tdata.py
Purpose: Streaming ingestion and normalization.  
Connects to Event Hubs, Kafka, or Databricks Autoloader.  
Writes raw telemetry to Bronze, cleans to Silver, adds timestamps, computes sequence gaps, and manages checkpoint recovery.  
Acts as the data bridge between the embedded C++ telemetry producers and the analytics pipeline.

---

### 2. interpreting data streams.py
Purpose: Defines schema for flight telemetry ingestion.  
Ensures every telemetry field matches the C++ firmware struct layout (IMU, gyroscope, magnetometer, servo commands, RSSI, EMI).

---

### 3. quality expectations.md
Purpose: Defines thresholds for telemetry validation.  
- At least 10 samples per 100 ms window  
- No missing IMU/Gyro values  
- Gyro standard deviation < 80 deg/s  
- EMI maximum < 0.8  

Composite quality score (0–100): 40% completeness, 30% noise, 30% EMI.

---

### 4. quality.sql
Purpose: Converts quality rules into SQL logic.  
Aggregates telemetry data in 100 ms windows, computes sample counts, null checks, standard deviations, and EMI values, and generates Boolean validation flags per window.

---

### 5. data scores and quality.py
Purpose: Generates quality scores in real time from Silver telemetry.  
Writes rolling 100 ms scores and 10-minute summaries to Gold tables.  
Provides quantitative assessments of sensor reliability, EMI peaks, and stability performance summaries.

---

### 6. streaming features.py
Purpose: Derives control performance features.  
Computes roll, pitch, yaw, and servo jitter, EMI averages, and anomaly flags.  
Calculates correlations between EMI and control jitter to quantify interference effects on flight stability.

---

### 7. tables function.py
Purpose: Runs Delta Lake optimization.  
Executes `OPTIMIZE ... ZORDER BY (flight_id)` to enhance query performance across all layers.

---

### 8. databricks table.sql
Purpose: Registers Gold Delta tables.  
Creates managed Delta tables for `qc_windowed`, `qc_flight_summary`, and `features` ensuring consistent schema and governance.

---

### 9. dashboard.sql
Purpose: Builds analytical dashboards.  
Defines SQL views for visualization and trend analysis:  
- `flight_summary`: QC and EMI statistics per flight  
- `emi_vs_jitter`: EMI versus control jitter correlation  
- `qc_trends`: Hourly QC evolution  

---

### 10. JSONC pipeline flight controls and EMI.jsonc
Purpose: Configuration and metadata definition.  
Specifies IMU/servo mappings, EMI calibration, and telemetry topic metadata.

---

## Relation to the C++ Flight Controls and EMI Firmware

This Databricks pipeline complements the C++ Flight Stabilization Firmware by providing cloud-based real-time analytics for the same telemetry streams.

| Component | Firmware (C++) | Analytics Pipeline (PySpark) |
|------------|----------------|------------------------------|
| Sensor Data | IMU, Gyro, RSSI, EMI | Parsed via telemetry_schema |
| Control Surfaces | Aileron, Elevator, Rudder | Jitter and anomaly analysis |
| EMI Monitoring | ADC-based EMI sensor | Correlation and impact analysis |
| Output | Servo control signals | Dashboards and statistical quality metrics |

Together, they form a comprehensive telemetry ecosystem where the firmware manages flight control and stabilization, while the Databricks system validates, scores, and visualizes telemetry performance and interference patterns.

---

## Example Visualizations

- Quality Score Distribution (histogram)  
- Hourly Quality Trends (line chart)  
- EMI vs Control Jitter (scatter plot)  
- Correlation Heatmap (EMI vs Roll/Pitch/Yaw)

---

## Technologies Used

- PySpark Structured Streaming  
- Delta Lake and Databricks SQL  
- Spark SQL with Window Functions  
- Azure Event Hubs / Kafka  
- JSONC Metadata Configuration  
- Power BI and Databricks Dashboards  

---

## Execution Instructions

1. Configure Databricks mounts and secrets.  
2. Run `Ingest_of_flight_Tdata.py` to initiate streaming ingestion.  
3. Execute `data scores and quality.py` and `streaming features.py` for QC and feature extraction.  
4. Register and optimize tables using `databricks table.sql` and `tables function.py`.  
5. Build and view dashboards using `dashboard.sql`.

---

## Future Enhancements

- Integration of altitude and airspeed sensors.  
- Predictive modeling for EMI-induced instability.  
- Long-term analytics via Azure Synapse integration.  
- Multi-flight comparison dashboards for fleet operations.

---

## Overall Project Outcomes

### End-to-End Integration
The C++ firmware reliably transmits telemetry data that the Databricks pipeline ingests, cleans, validates, and analyzes.

### Quality Control Results
The project achieves >95% data completeness, <5% noise, and negligible EMI contamination — yielding high QC scores per window.

### Performance Insights
The analytical layer successfully detects anomalies, measures control jitter, and correlates EMI with servo behavior in real time.

### Operational Readiness
The architecture is fully scalable for multi-flight ingestion, with the ability to expand into predictive maintenance and fleet analysis dashboards.

---

## Resources and References

The following resources, documentation, and frameworks were instrumental in the development of this project:

### Databricks and Delta Lake
- Databricks Documentation – [https://docs.databricks.com](https://docs.databricks.com)  
- Delta Lake Format Guide – [https://delta.io](https://delta.io)  
- Structured Streaming Programming Guide – [https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

### PySpark and Data Engineering
- PySpark API Reference – [https://spark.apache.org/docs/latest/api/python](https://spark.apache.org/docs/latest/api/python)  
- Apache Kafka Quickstart – [https://kafka.apache.org/quickstart](https://kafka.apache.org/quickstart)  
- Azure Event Hubs Integration for Spark – [https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-spark-structured-streaming](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-spark-structured-streaming)

### Embedded Systems and Flight Telemetry
- Arduino IMU and Sensor Integration – [https://www.arduino.cc/en/Guide](https://www.arduino.cc/en/Guide)  
- PCA9685 PWM Servo Driver Datasheet – NXP Semiconductors  
- MPU6050/MPU9250 IMU Datasheet and Register Map – InvenSense  
- RC Aircraft Control Principles – Experimental RC Flight Research Archives  

### Data Quality and Validation
- Great Expectations Framework – [https://greatexpectations.io](https://greatexpectations.io)  
- Delta Live Tables and Streaming QC Pipelines – Databricks Engineering Blog  
- ISO 19157 Data Quality Standard (for methodology reference)

### Visualization and Analysis
- Power BI Desktop and Databricks SQL Dashboards  
- Matplotlib, Pandas, and Seaborn for Statistical Plotting  
- Correlation and Signal Noise Filtering Techniques from Applied Data Science Texts

### Software and Development Tools
- Visual Studio Code and Databricks Repos for collaborative development  
- GitHub for version control and documentation  
- Azure Storage Gen2 for streaming data ingestion and checkpointing  
- Jupyter Notebooks for validation and analysis of Spark outputs

### Academic and Conceptual References
- “Principles of Flight Instrumentation and Control” – McGraw-Hill Education  
- “Data Engineering with Apache Spark, Delta Lake, and Databricks” – O’Reilly Media  
- Research papers on EMI impact in UAV control systems (IEEE Xplore Library)

---

This collection of resources supported the implementation of the project’s full lifecycle from embedded telemetry acquisition to distributed data processing, feature engineering, and operational dashboard deployment.

---

## License

MIT License © 2025 — Alejandro Choapa
