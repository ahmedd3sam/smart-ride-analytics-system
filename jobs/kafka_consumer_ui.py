#---------------------------------------------------------

# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# # ----------------------------
# # Create Flink environments
# # ----------------------------
# env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(1)

# settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
# t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# # ----------------------------
# # Kafka source table
# # ----------------------------
# t_env.execute_sql("""
# CREATE TABLE taxi_trips (
#     vendor_id STRING,
#     fare_amount DOUBLE,
#     tip_amount DOUBLE,
#     pickup_datetime STRING,
#     dropoff_datetime STRING,
#     passenger_count INT,
#     trip_distance DOUBLE
# ) WITH (
#     'connector' = 'kafka',
#     'topic' = 's3-taxi-trips',
#     'properties.bootstrap.servers' = 'kafka:9092',
#     'properties.group.id' = 'flink-group',
#     'scan.startup.mode' = 'earliest-offset',
#     'format' = 'json'
# )
# """)

# # ----------------------------
# # PostgreSQL Dashboard Table ONLY
# # ----------------------------
# t_env.execute_sql("""
# CREATE TABLE taxi_trips_dashboard (
#     vendor_id STRING,
#     trip_distance DOUBLE,
#     fare_amount DOUBLE,
#     tip_amount DOUBLE,
#     total_fare DOUBLE,
#     trip_duration_minutes DOUBLE,
#     trip_speed DOUBLE,
#     fare_per_distance DOUBLE,
#     fare_per_time DOUBLE,
#     simulated_event_time TIMESTAMP
# ) WITH (
#     'connector' = 'jdbc',
#     'url' = 'jdbc:postgresql://postgres:5432/taxi_db',
#     'table-name' = 'taxi_trips_dashboard',
#     'username' = 'user',
#     'password' = 'password'
# )
# """)

# # ----------------------------
# # Cleaning
# # ----------------------------
# t_env.execute_sql("""
# CREATE VIEW taxi_trips_cleaned AS
# SELECT *
# FROM taxi_trips
# WHERE fare_amount IS NOT NULL
#   AND trip_distance IS NOT NULL
#   AND trip_distance > 0
#   AND passenger_count > 0
#   AND pickup_datetime IS NOT NULL
#   AND dropoff_datetime IS NOT NULL
# """)

# # ----------------------------
# # Enriched Transformations with Corrected Timestamp Calculations
# # ----------------------------
# t_env.execute_sql("""
# CREATE VIEW taxi_trips_enriched AS
# SELECT
#     vendor_id,
#     trip_distance,
#     fare_amount,
#     tip_amount,

#     (fare_amount + tip_amount) AS total_fare,

#     -- Trip duration in minutes
#     TIMESTAMPDIFF(MINUTE, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) AS trip_duration_minutes,

#     -- Trip speed in miles/hour
#     CASE 
#         WHEN TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) = 0 
#         THEN NULL
#         ELSE trip_distance / (TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) / 3600.0)
#     END AS trip_speed,

#     -- Fare per distance in $/mile
#     CASE 
#         WHEN trip_distance = 0 THEN NULL
#         ELSE (fare_amount + tip_amount) / trip_distance
#     END AS fare_per_distance,

#     -- Fare per time in $/hour
#     CASE 
#         WHEN TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) = 0
#         THEN NULL
#         ELSE (fare_amount + tip_amount) / (TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) / 3600.0)
#     END AS fare_per_time,

#     CURRENT_TIMESTAMP AS simulated_event_time

# FROM taxi_trips_cleaned
# WHERE CAST(dropoff_datetime AS TIMESTAMP) > CAST(pickup_datetime AS TIMESTAMP)
# """)

# # ----------------------------
# # Insert ONLY into dashboard table
# # ----------------------------
# t_env.execute_sql("""
# INSERT INTO taxi_trips_dashboard
# SELECT
#     vendor_id,
#     trip_distance,
#     fare_amount,
#     tip_amount,
#     total_fare,
#     trip_duration_minutes,
#     trip_speed,
#     fare_per_distance,
#     fare_per_time,
#     simulated_event_time
# FROM taxi_trips_enriched
# """)

# # ----------------------------
# # Execute Flink job
# # ----------------------------
# # env.execute("Flink Kafka Taxi Dashboard Job")

# ---------------------------------------------------------------------------------------------- #

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ----------------------------
# Create Flink environments
# ----------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# ----------------------------
# Kafka source table
# ----------------------------
t_env.execute_sql("""
CREATE TABLE taxi_trips (
    vendor_id STRING,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    pickup_datetime STRING,
    dropoff_datetime STRING,
    passenger_count INT,
    trip_distance DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 's3-taxi-trips',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# ----------------------------
# PostgreSQL Dashboard Table
# ----------------------------
t_env.execute_sql("""
CREATE TABLE taxi_trips_dashboard (
    taxi_id INT,
    vendor_id STRING,
    passenger_count INT,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    total_fare DOUBLE,
    trip_duration_minutes DOUBLE,
    trip_speed DOUBLE,
    fare_per_distance DOUBLE,
    fare_per_time DOUBLE,
    simulated_event_time TIMESTAMP
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/taxi_db',
    'table-name' = 'taxi_trips_dashboard',
    'username' = 'user',
    'password' = 'password'
)
""")

# ----------------------------
# Cleaning
# ----------------------------
t_env.execute_sql("""
CREATE VIEW taxi_trips_cleaned AS
SELECT *
FROM taxi_trips
WHERE fare_amount IS NOT NULL
  AND trip_distance IS NOT NULL
  AND trip_distance > 0
  AND passenger_count > 0
  AND pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
""")

# ----------------------------
# Enriched Transformations with type casts
# ----------------------------
t_env.execute_sql("""
CREATE VIEW taxi_trips_enriched AS
SELECT
    CAST(FLOOR(RAND() * 10 + 1) AS INT) AS taxi_id,  -- Random taxi ID between 1 and 10
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    (fare_amount + tip_amount) AS total_fare,

    -- Trip duration in minutes as DOUBLE
    CAST(TIMESTAMPDIFF(MINUTE, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) AS DOUBLE) AS trip_duration_minutes,

    -- Trip speed in miles/hour
    CASE 
        WHEN TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) = 0 
        THEN NULL
        ELSE trip_distance / (TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) / 3600.0)
    END AS trip_speed,

    -- Fare per distance in $/mile
    CASE 
        WHEN trip_distance = 0 THEN NULL
        ELSE (fare_amount + tip_amount) / trip_distance
    END AS fare_per_distance,

    -- Fare per time in $/hour
    CASE 
        WHEN TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) = 0
        THEN NULL
        ELSE (fare_amount + tip_amount) / (TIMESTAMPDIFF(SECOND, CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP)) / 3600.0)
    END AS fare_per_time,

    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS simulated_event_time

FROM taxi_trips_cleaned
WHERE CAST(dropoff_datetime AS TIMESTAMP) > CAST(pickup_datetime AS TIMESTAMP)
""")

# ----------------------------
# Insert ONLY into dashboard table
# ----------------------------
t_env.execute_sql("""
INSERT INTO taxi_trips_dashboard
SELECT
    taxi_id,
    vendor_id,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_fare,
    trip_duration_minutes,
    trip_speed,
    fare_per_distance,
    fare_per_time,
    simulated_event_time
FROM taxi_trips_enriched
""")

# ----------------------------
# Execute Flink job
# ----------------------------
env.execute("Flink Kafka Taxi Dashboard Job")
