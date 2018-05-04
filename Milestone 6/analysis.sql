#revenue generated over time
SELECT 'Austin' as city, price
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
ORDER BY date;

SELECT 'Austin' as city, COUNT(listing_id), date
FROM `kapetanakissaenzcs327e.austin_temp.Listing`
GROUP BY date;
