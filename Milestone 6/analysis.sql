#revenue generated over time
SELECT 'Austin' as city, l.price * l.minimum_nights * l.number_of_reviews as revenue, c.date
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id
ORDER BY c.date;

#Number of rentals available grouped by the date
SELECT 'Austin' as city, COUNT(l.id) as num_of_rentals , c.date
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id
GROUP BY c.date
ORDER BY c.date;

#occupancy rate over time
SELECT 'Austin' as city, l.minimum_nights * l.number_of_reviews as days_occupied
FROM `kapetanakissaenzcs327e.austin_temp.Listing` l JOIN `kapetanakissaenzcs327e.austin_temp.Calendar` c ON l.id = c.listing_id;

