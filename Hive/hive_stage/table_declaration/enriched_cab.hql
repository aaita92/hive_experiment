create database if not exists processed_data;
use processed_data;
create external table if not exists cab_data (
    lat double,
    lon double,
    occupation int,
    time timestamp,
    data date,
    excluded_flag int,
    passenger_status int,
    counter_interval int
    )
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/processed_data.db/data_ready';