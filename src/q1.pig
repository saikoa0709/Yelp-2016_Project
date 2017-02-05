register Downloads/elephant-bird-hadoop-compat-4.13.jar
register Downloads/elephant-bird-core-4.13.jar
register Downloads/elephant-bird-pig-4.13.jar

q1Data = Load './yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q1Count = FOREACH q1Data GENERATE users#'city' AS city, flatten(users#'categories') AS category, (int)users#'review_count' AS review_count, (double) users#'latitude' AS latitude, (double) users#'longitude' AS longitude;

q1Count1 = FILTER q1Count BY latitude < 49.38447 AND latitude > 24.5208 AND longitude <-66.95 AND longitude > -124.7666;

q1by_city_category = GROUP q1Count1 BY (city, category);

q1Result = FOREACH q1by_city_category GENERATE flatten(group) AS (city,category), SUM(q1Count1.review_count);

STORE q1Result INTO './midterm1';


