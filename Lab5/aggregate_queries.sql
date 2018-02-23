#List the average price for airbnb listings that Houses, along with the reviewer name, and the listing id.
#Also order the results by average price highest-to-lowest.
#Include all the listings and their average prices that dont have reviews.
SELECT avg(l.price), r.reviewer_name, r.comments, l.id
FROM Listing l LEFT OUTER JOIN Review r ON l.id = r.listing_id
GROUP BY r.comments
ORDER BY avg(price) desc;

#
