#revenue generated over time and the # of listings
SELECT 'Austin' as city, price, COUNT(listing_id)
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
ORDER BY date;

