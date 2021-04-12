
-- SQL query to check the Hadoop results:

USE Airline;

SELECT Carrier, MIN(DepDelayMinutes), MAX(DepDelayMinutes), AVG(DepDelayMinutes)
FROM On_Time_On_Time_Performance_2016_1
GROUP BY Carrier;

/*
# Import data into MySQL:
mysql -B -D Airline -e 'SOURCE airline-query.sql'

# Import from MySQL into HDFS:
sqoop import \
    --connect 'jdbc:mysql://training:training@localhost/Airline' \
    --table On_Time_On_Time_Performance_2016_1 \
    --columns 'Carrier,DepDelayMinutes' \
    --split-by Carrier \
    --fields-terminated-by "\t" \
    --bindir /tmp \
    --target-dir /airline-sqoop

# Hadoop streaming script:
hadoop jar \
    $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -mapper /bin/cat \
    -reducer reducer.py \
    -input /airline-sqoop \
    -output /airline-ontime
*/
