#
CREATE VIEW  v_Revenue_Crossover AS
(SELECT DATE_TRUNC(Calendar.date, MONTH) as AirbnbMonth
FROM `kapetanakissaenzcs327e.austin_temp.Calendar`)
UNION ALL
(SELECT 'Boston' as city, name, listing_url, price
FROM `kapetanakissaenzcs327e.boston_temp.Listing`
ORDER BY price DESC
LIMIT 10)
UNION ALL
(SELECT 'Portland' as city, name, listing_url, price
FROM `kapetanakissaenzcs327e.portland_temp.Listing`
ORDER BY price DESC
LIMIT 10);
