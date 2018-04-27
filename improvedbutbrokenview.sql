CREATE VIEW  v_Revenue_Crossover AS
(SELECT date, zipcode, bedrooms, airbnb_price_day, zillow_price_month,
	(zillow_price_month/airbnb_price_day) AS crossover_pt
FROM 
(
	(SELECT DATE_TRUNC(Calendar.date, MONTH) as date,
  
		CASE 
			WHEN Calendar.price is NULL THEN Listing.price
			ELSE Calendar.price 
		END as price1,
    
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) as airbnb_price_day,
		Listing.zipcode,
		Listing.bedrooms
	FROM `kapetanakissaenzcs327e.austin_temp.Calendar` Calendar JOIN `kapetanakissaenzcs327e.austin_temp.Listing` Listing ON Listing.id=Calendar.listing_id
	WHERE Listing.room_type = 'Entire home/apt' AND Listing.bedrooms > 0)
  
	UNION ALL
	(SELECT date, zipcode, 
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) AS zillow_price_month
	FROM `kapetanakissaenzcs327e.zillow.Rental_Price_1Bedroom`)
  
	UNION ALL
	(SELECT date, zipcode, 
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) AS zillow_price_month
	FROM `kapetanakissaenzcs327e.zillow.Rental_Price_2Bedroom`)
  
	UNION ALL
	(SELECT date, zipcode, 
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) AS zillow_price_month
	FROM `kapetanakissaenzcs327e.zillow.Rental_Price_3Bedroom`)
  
	UNION ALL
	(SELECT date, zipcode, 
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) AS zillow_price_month
	FROM `kapetanakissaenzcs327e.zillow.Rental_Price_4Bedroom`)
  
	UNION ALL
	(SELECT date, zipcode, 
		PERCENTILE_CONT(price1, 0.5) OVER(PARTITION BY date) AS zillow_price_month
	FROM `kapetanakissaenzcs327e.zillow.Rental_Price_5BedroomOrMore`)
)
)