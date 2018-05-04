#revenue generated over time
SELECT 'Austin' as city, l.price
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id
ORDER BY c.date;

#Number of rentals available grouped by the date
SELECT 'Austin' as city, COUNT(l.id), c.date
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id
GROUP BY c.date;

#occupancy rate over time
SELECT 'Austin' as city, l.minimum_nights * l.number_of_reviews as occupancy_rate
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id
ORDER BY c.date;

