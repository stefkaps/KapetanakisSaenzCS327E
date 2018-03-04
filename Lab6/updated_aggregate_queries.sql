#List the average price for airbnb listings that Houses, along with the reviewer name, and the listing id.
#Also order the results by average price highest-to-lowest.
#Include all the listings and their average prices that dont have reviews.
SELECT avg(l.price), r.reviewer_name, r.comments, l.id
FROM Listing l LEFT OUTER JOIN Review r ON l.id = r.listing_id
GROUP BY r.comments r.reviewer_name, l.id
ORDER BY avg(l.price);

#Lists airbnb listings id that have the lowest price by zipcode, and orders the results by price highest to lowest.
SELECT min(price), id, zipcode
FROM Listing
GROUP BY zipcode, id
ORDER BY min(price) desc;

# Retrieves Host id and averages the response rate of hosts by state that are below 75%
SELECT avg(h.response_rate), l.host_id
FROM Host h JOIN Listing l ON l.host_id = h.id
GROUP BY l.state, l.host_id
HAVING avg(h.response_rate) <75;

# Sums all prices of listings by date with prices > 0 
SELECT c.date1, sum(l.price)
FROM Calendar c JOIN Listing l ON c.listing_id = l.id
GROUP BY c.date1
HAVING sum(l.price) > 0;

#Pulls the average 30 day availability for each host where 30 day availability is larger than 5
SELECT l.host_id, avg(cs.availability_30) 
FROM Calendar_summary cs JOIN Listing l ON cs.listing_id = l.id 
WHERE cs.availability_30 > 5 
GROUP BY l.host_id;

#Averages the number of reviews by city per listing
SELECT avg(number_of_reviews), city
FROM Listing 
WHERE number_of_reviews > 0 
GROUP BY city;
