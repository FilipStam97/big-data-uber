### Big data uber

## Commands:
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --min-distance 5 --sort-by trip_miles --desc --limit 10

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by PULocationID --limit 10

## -------------------------------------

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute base_passenger_fare --group-by pickup_zone --limit 20

## --------------------------------------

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by pickup_hour --limit 24

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute base_passenger_fare --group-by pickup_weekday --limit 7

Useful for presenation:
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_time --group-by pickup_borough --limit 10

python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --hour 18 --min-distance 5 --sort-by trip_miles --desc --limit 10

## Spark test
python src\spark_main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by pickup_borough --limit 10

python src\spark_main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --pickup-zone 132 --min-distance 5 --sort-by trip_miles --desc --limit 10


## Check if datanode is registered
docker exec -it namenode hdfs dfsadmin -report


create project directories:

docker exec -it namenode hdfs dfs -mkdir -p /uber/raw
docker exec -it namenode hdfs dfs -mkdir -p /uber/lookup

verify: docker exec -it namenode hdfs dfs -ls /uber

list raw data: docker exec namenode ls /data/raw
docker exec namenode ls /data/lookup


upload files to hdfs:
docker exec namenode hdfs dfs -put /data/lookup/taxi_zone_lookup.csv /uber/lookup/

just the first file: docker exec namenode hdfs dfs -put /data/raw/fhvhv_tripdata_2021-01.parquet /uber/raw/

verify: docker exec namenode hdfs dfs -ls -h /uber/raw
verify: docker exec namenode hdfs dfs -ls -h /uber/lookup

hdfs usage: docker exec namenode hdfs dfs -du -h /uber/raw

upload the whole raw folder: docker exec namenode bash -lc "hdfs dfs -put /data/raw/*.parquet /uber/raw/"
delete file if needed: docker exec namenode hdfs dfs -rm /uber/raw/fhvhv_tripdata_2021-01.parquet

verify all the data: docker exec namenode hdfs dfs -ls -h /uber/raw
check size: docker exec namenode hdfs dfs -du -h -s /uber/raw
integrity check: docker exec namenode hdfs fsck /uber/raw -files -blocks

test with first cluster job:
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw/fhvhv_tripdata_2021-01.parquet --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv stats --attribute trip_miles --group-by pickup_borough --limit 10


test full trip functionallity:
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw/fhvhv_tripdata_2021-01.parquet --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv trips --start-date 2021-01-15 --end-date 2021-01-15 --hour 18 --min-distance 5 --sort-by trip_miles --desc --columns pickup_datetime pickup_borough pickup_zone dropoff_borough dropoff_zone trip_miles tips --limit 10


test full stats func:
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw/fhvhv_tripdata_2021-01.parquet --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv stats --attribute trip_miles --group-by pickup_borough --limit 10

run for the whole year:
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv stats --attribute trip_miles --group-by pickup_borough --limit 10


#### ---------------------------------------------------------------------------


# NYC Uber Big Data Analysis

Big Data project for processing NYC High Volume For-Hire Vehicle (HVFHV) trip data using **Apache Spark, HDFS, Docker and Python**.

The project contains:
- standalone implementation using Pandas
- distributed implementation using PySpark
- Spark cluster with 1 master and 2 workers
- HDFS storage with NameNode and DataNode
- separate Docker container for the Spark application

The complete 2021 dataset contains **174,596,652 trips**.

## Architecture

```text
                    spark-app
                  (Spark Driver)
                       |
                       | spark-submit
                       v
                  Spark Master
                       |
                 +-----+-----+
                 |           |
                 v           v
             Worker 1     Worker 2
                 \           /
                  \         /
                   v       v
                      HDFS
                NameNode + DataNode
```

All services run as Docker containers on the same Docker Compose network.

## Start the project

```powershell
docker compose up -d
```

Check containers:

```powershell
docker compose ps
```

Spark UI:

```text
http://localhost:8080
```

HDFS UI:

```text
http://localhost:9870
```

## HDFS

Check HDFS:

```powershell
docker exec namenode hdfs dfsadmin -report
```

Create directories:

```powershell
docker exec namenode hdfs dfs -mkdir -p /uber/raw
docker exec namenode hdfs dfs -mkdir -p /uber/lookup
```

Upload lookup data:

```powershell
docker exec namenode hdfs dfs -put /data/lookup/taxi_zone_lookup.csv /uber/lookup/
```

Upload a monthly dataset:

```powershell
docker exec namenode hdfs dfs -put /data/raw/fhvhv_tripdata_2021-01.parquet /uber/raw/
```

List HDFS data:

```powershell
docker exec namenode hdfs dfs -ls -h /uber/raw
```

## Standalone Pandas

January statistics:

```powershell
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet stats --attribute trip_miles --group-by pickup_borough --limit 10
```

Filtering and sorting example:

```powershell
python src\main.py --data data\raw\fhvhv_tripdata_2021-01.parquet trips --start-date 2021-01-15 --end-date 2021-01-15 --hour 18 --min-distance 5 --sort-by trip_miles --desc --columns pickup_datetime pickup_borough pickup_zone dropoff_borough dropoff_zone trip_miles tips --limit 10
```

## Spark

Rebuild the application after code changes:

```powershell
docker compose up -d --build spark-app
```

January statistics:

```powershell
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw/fhvhv_tripdata_2021-01.parquet --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv stats --attribute trip_miles --group-by pickup_borough --limit 10
```

Full-year statistics:

```powershell
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv stats --attribute trip_miles --group-by pickup_borough --limit 10
```

Filtering and sorting example:

```powershell
docker exec spark-app /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-app /opt/app/spark_main.py --data hdfs://namenode:9000/uber/raw/fhvhv_tripdata_2021-01.parquet --zones hdfs://namenode:9000/uber/lookup/taxi_zone_lookup.csv trips --start-date 2021-01-15 --end-date 2021-01-15 --hour 18 --min-distance 5 --sort-by trip_miles --desc --columns pickup_datetime pickup_borough pickup_zone dropoff_borough dropoff_zone trip_miles tips --limit 10
```

## Performance

| Dataset | Records | Pandas | Spark |
|---|---:|---:|---:|
| January 2021 | 11,908,468 | 32.51 s | 15.97 s |
| Full 2021 | 174,596,652 | Insufficient memory | 37.34 s |

For the full dataset, the standalone Pandas implementation exceeded available memory while combining the monthly datasets. Spark successfully processed the complete dataset using the distributed cluster.

## Stop the project

```powershell
docker compose down
```