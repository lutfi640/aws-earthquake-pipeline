
# AWS Earthquake Data Pipeline

Automated end-to-end data pipeline to ingest, transform, and store real-time earthquake data from USGS API to S3, orchestrated by Apache Airflow.

## Architecture

```mermaid
graph LR
    A[USGS API] --> B[Airflow/EC2]
    B --> C[(S3 Bronze)]
    C --> D[AWS Lambda]
    D --> E[(S3 Silver)]
    E --> F[Amazon Athena]
```
