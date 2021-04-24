
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

SELECT
    SPLIT(airline_names.AirlineName, ':')[0],
    AVG(airline_delays.ArrDelayMinutes)
FROM airline_delays
JOIN airline_names ON (airline_names.AirlineID = airline_delays.AirlineID)
GROUP BY airline_names.AirlineName;

-- Alaska Airlines Inc.    6.978952625570776
-- American Airlines Inc.  10.695254069511659
-- Delta Air Lines Inc.    9.46074961033664
-- ExpressJet Airlines Inc.        10.915249208860759
-- Frontier Airlines Inc.  10.652892561983471
-- Hawaiian Airlines Inc.  4.184370015948963
-- JetBlue Airways 17.283021432779012
-- SkyWest Airlines Inc.   15.167911974333025
-- Southwest Airlines Co.  7.5740563477574385
-- Spirit Air Lines        18.634635101127785
-- United Air Lines Inc.   9.674841669055748
-- Virgin America  15.657723265619012
