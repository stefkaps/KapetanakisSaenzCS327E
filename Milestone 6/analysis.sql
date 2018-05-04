#revenue generated over time
SELECT 'Austin' as city, price
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
ORDER BY date;

#Number of rentals available grouped by the date
SELECT 'Austin' as city, COUNT(listing_id), date
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
GROUP BY date;

#occupancy rate over time
SELECT 'Austin' as city, availability_365
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
ORDER BY date;

