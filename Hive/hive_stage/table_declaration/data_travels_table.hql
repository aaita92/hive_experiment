create database if not exists processed_data;
use processed_data;
create external table if not exists travels (
    counter_travels int,
    passenger_status int,
    distance double,
    time_delta_s int
    )
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/processed_data.db/travels';