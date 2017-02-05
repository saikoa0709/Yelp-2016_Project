# Yelp-2016_Project

Use Hadoop technologies to analyze the Yelp 2016 challenge dataset: https://www.yelp.com/dataset_challenge.

1. Summarize the number of reviews by US city, by business category.

2. Rank the cities by # of stars for each category, by city.

3. What is the average rank (stars) for businesses within 5 miles of Carnegie Mellon University in Pittsburgh, PA, by type?
Center: Carnegie Mellon University, Pitsburgh, PA
Latitude: 40-26'28'' N, Longitude: 079-56'34'' W
Decimal Degrees: Latitude: 40.4411801, Longitude: -79.9428294
The bounding box for this problem is ~5 miles, which we will loosely define as 5 minutes. So the bounding box is a square box, 10 minutes each side (of longitude and latitude), with CMU at the center.

4. Rank reviewers by number of reviews. For the top 10 reviewers, show their average number of stars, by category.

5. For the top 10 and bottom 10 food business near CMU (in terms of stars), summarize star rating by of month.


JSON loader source: https://github.com/twitter/elephant-bird
