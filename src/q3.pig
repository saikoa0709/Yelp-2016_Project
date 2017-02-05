register Downloads/elephant-bird-hadoop-compat-4.13.jar
register Downloads/elephant-bird-core-4.13.jar
register Downloads/elephant-bird-pig-4.13.jar

q3Data = Load './yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q3Count = FOREACH q3Data GENERATE users#'type' AS type, (double)users#'stars' AS stars, (double) users#'latitude' AS latitude, (double) users#'longitude' AS longitude;

q3Filter = FILTER q3Count BY latitude < 40.5135401 AND latitude > 40.3688201 AND longitude <-79.8477494 AND longitude > -80.0379094;

q3Group = GROUP q3Filter BY type;

q3Result = FOREACH q3Group GENERATE group AS type, AVG(q3Filter.stars);

STORE q3Result INTO './midterm3';


