register Downloads/elephant-bird-hadoop-compat-4.13.jar
register Downloads/elephant-bird-core-4.13.jar
register Downloads/elephant-bird-pig-4.13.jar

q4Data = Load './yelp_academic_dataset_user.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q4Data1 = Load './yelp_academic_dataset_review.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q4Data2 = Load './yelp_academic_dataset_business.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true') AS (users: map[]);

q4Count = FOREACH q4Data GENERATE users#'name' AS name, users#'user_id' AS id, (int)users#'review_count' AS review_count, (double)users#'average_stars' AS av_stars;

q4Count1 = FOREACH q4Data1 GENERATE users#'user_id' AS id, users#'business_id' AS b_id;

q4Count2 = FOREACH q4Data2 GENERATE users#'business_id' AS b_id, flatten(users#'categories') AS category; 

/****Order by review_count and limit number****/
 
q4top = ORDER q4Count BY review_count desc;

q4top_limit = LIMIT q4top 10;

/****First join user&review json by user_id****/

q4Join = JOIN q4top_limit BY id, q4Count1 BY id;

q4tmp = FOREACH q4Join GENERATE name, review_count, av_stars, b_id;

/****Second join business json by business_id****/

q4Join1 = JOIN q4tmp by b_id, q4Count2 by b_id;

q4Join_Category = GROUP q4Join1 BY (name, category);

q4Result = FOREACH q4Join_Category GENERATE flatten(group) AS (name, category), AVG(q4Join1.av_stars);

STORE q4Result INTO './midterm4';
