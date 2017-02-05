register Downloads/elephant-bird-hadoop-compat-4.13.jar
register Downloads/elephant-bird-core-4.13.jar
register Downloads/elephant-bird-pig-4.13.jar

q2Data = Load './yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q2Count = FOREACH q2Data GENERATE users#'city' AS city, flatten(users#'categories') AS category, (double)users#'stars' AS stars;

q2by_city = group q2Count BY city;

q2by_city_and_category = group q2Count BY (city, category);

/****sum total stars by city****/

q2by_city_count = FOREACH q2by_city GENERATE flatten(group) AS city, SUM(q2Count.stars) AS star_city;

/****sum total stars by category****/

q2by_city_and_category_count = FOREACH q2by_city_and_category GENERATE flatten(group) AS (city,category), SUM(q2Count.stars) AS star;

/****Join total stars by city and total stars by category****/

q2join = JOIN q2by_city_count BY city, q2by_city_and_category_count BY city;

q2tmp = FOREACH q2join GENERATE $0, star_city, category, star;

q2Result = ORDER q2tmp BY star_city desc, star desc;

STORE q2Result INTO './midterm2';
