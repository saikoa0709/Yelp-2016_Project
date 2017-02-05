register Downloads/elephant-bird-hadoop-compat-4.13.jar
register Downloads/elephant-bird-core-4.13.jar
register Downloads/elephant-bird-pig-4.13.jar

q5Data = Load './yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q5Data1 = Load './yelp_academic_dataset_review.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q5Count = FOREACH q5Data GENERATE users#'name' AS name, users#'business_id' AS business_id, flatten(users#'categories') AS category, (double)users#'stars' AS stars, (double) users#'latitude' AS latitude, (double) users#'longitude' AS longitude;

q5Count1 = FOREACH q5Data1 GENERATE users#'business_id' AS business_id, (double)users#'stars' AS stars, (chararray)users#'date'AS date;
q5Count1_Month = FOREACH q5Count1 GENERATE $0,$1,$2, SUBSTRING(date,5,7) AS month;


q5Filter = FILTER q5Count BY latitude < 40.5135401 AND latitude > 40.3688201 AND longitude <-79.8477494 AND longitude > -80.0379094 AND (chararray)category == 'Food';

/****Order and Limit top10 and bottom10 food business****/

q5top = ORDER q5Filter BY stars asc;
q5bottom = ORDER q5Filter BY stars desc;
q5top_l = LIMIT q5top 10;
q5bottom_l = LIMIT q5bottom 10;
q5Union = UNION q5top_l, q5bottom_l;

/****Access date by joining review json by business_id****/
q5Join = JOIN q5Union BY business_id, q5Count1_Month BY business_id;

/****Group month and reorder the top10 and bottom10 food businesses by month****/
q5tmp = FOREACH q5Join GENERATE name,category,q5Union::stars, month;
q5tmp1 = GROUP q5tmp BY month;
q5tmp2 = FOREACH q5tmp1 {sorted = ORDER q5tmp by stars desc; GENERATE flatten(group) as month, flatten(sorted);};

STORE q5tmp2 INTO './midterm5';
