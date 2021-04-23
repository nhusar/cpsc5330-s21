
CREATE EXTERNAL TABLE IF NOT EXISTS airline_delays (
    AirlineID int,
    ArrDelayMinutes float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/sqoop-airline-delays';

CREATE EXTERNAL TABLE IF NOT EXISTS airline_names (
    AirlineID int,
    AirlineName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '/sqoop-airline-names';

SELECT SPLIT(n.AirlineName, ':')[0], AVG(d.ArrDelayMinutes)
FROM airline_delays d
JOIN airline_names n ON (n.AirlineID = d.AirlineID)
GROUP BY n.AirlineName;
