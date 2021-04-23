#!/bin/bash

mysql -G -B -e \
    'CREATE DATABASE IF NOT EXISTS Airline; USE Airline; \. /tmp/airline.sql'

sqoop import \
    --connect 'jdbc:mysql://training:training@localhost/Airline' \
    --table On_Time_On_Time_Performance_2016_1 \
    --columns 'AirlineID,ArrDelayMinutes' \
    --split-by AirlineID \
    --fields-terminated-by "\t" \
    --bindir /tmp \
    --target-dir /sqoop-airline-delays

sqoop import \
    --connect 'jdbc:mysql://training:training@localhost/Airline' \
    --table L_AIRLINE_ID \
    --columns 'Code,Description' \
    --split-by Code \
    --fields-terminated-by "\t" \
    --bindir /tmp \
    --target-dir /sqoop-airline-names
