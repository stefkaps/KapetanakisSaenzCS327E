#Austin - Revenue Crossover view
#Join together Austin's Calendar and Listings table data. 
#Then join them with the each's respective data from the different bedroom table in the Zillow dataset. 
#Once joins are made, bring it all together with UNIONS for bedrooms 1-5. 
(SELECT Distinct A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, 
	percentile_cont(B.price, 0.5) over (partition by B.date) as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
	FROM `kapetanakissaenzcs327e.austin_temp.Calendar`  c join `kapetanakissaenzcs327e.austin_temp.Listing`  l on c.listing_id = l.id
	WHERE bedrooms = 1 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_1Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.austin_temp.Calendar` c join `kapetanakissaenzcs327e.austin_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 2 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
JOIN `kapetanakissaenzcs327e.zillow.Rental_Price_2Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.austin_temp.Calendar`  c join `kapetanakissaenzcs327e.austin_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 3 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_3Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.austin_temp.Calendar`  c join `kapetanakissaenzcs327e.austin_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 4 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_4Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, 
	case when bedrooms >= 5 then 5 end as bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.austin_temp.Calendar`  c join `kapetanakissaenzcs327e.austin_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms >= 5 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_5BedroomOrMore`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
Order By airbnb_price_day

#Create Austin v_Revenue_Crossover
#Calculate the crossover_pt with all the data from previous
SELECT date, zipcode, bedrooms, airbnb_price_day, zillow_price_month, (zillow_price_month/airbnb_price_day) AS crossover_pt
FROM `kapetanakissaenzcs327e.austin_temp.temp_table`


#Portland - Revenue Crossover view
#Join together Portland's Calendar and Listings table data. 
#Then join them with the each's respective data from the different bedroom table in the Zillow dataset. 
#Once joins are made, bring it all together with UNIONS for bedrooms 1-5. 
(SELECT Distinct A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, 
	percentile_cont(B.price, 0.5) over (partition by B.date) as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.portland_temp.Calendar`  c join `kapetanakissaenzcs327e.portland_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 1 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_1Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.portland_temp.Calendar` c join `kapetanakissaenzcs327e.portland_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 2 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
JOIN `kapetanakissaenzcs327e.zillow.Rental_Price_2Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.portland_temp.Calendar`  c join `kapetanakissaenzcs327e.portland_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 3 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_3Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.portland_temp.Calendar`  c join `kapetanakissaenzcs327e.portland_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 4 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_4Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, 
	case when bedrooms >= 5 then 5 end as bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.portland_temp.Calendar`  c join `kapetanakissaenzcs327e.portland_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms >= 5 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_5BedroomOrMore`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
Order By airbnb_price_day

#Create Portland v_Revenue_Crossover
#Calculate the crossover_pt with all the data from previous
SELECT date, zipcode, bedrooms, airbnb_price_day, zillow_price_month, (zillow_price_month/airbnb_price_day) AS crossover_pt
FROM `kapetanakissaenzcs327e.portland_temp.temp_table`



#Boston - Revenue Crossover view
#Join together Boston's Calendar and Listings table data. 
#Then join them with the each's respective data from the different bedroom table in the Zillow dataset. 
#Once joins are made, bring it all together with UNIONS for bedrooms 1-5. 
(SELECT Distinct A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, 
	percentile_cont(B.price, 0.5) over (partition by B.date) as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.boston_temp.Calendar`  c join `kapetanakissaenzcs327e.boston_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 1 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_1Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.boston_temp.Calendar` c join `kapetanakissaenzcs327e.boston_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 2 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
JOIN `kapetanakissaenzcs327e.zillow.Rental_Price_2Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.boston_temp.Calendar`  c join `kapetanakissaenzcs327e.boston_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 3 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_3Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.boston_temp.Calendar`  c join `kapetanakissaenzcs327e.boston_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms = 4 AND room_type = 'Entire home/apt' AND zipcode is not null) as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_4Bedroom`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
UNION ALL 
(SELECT A.date, A.zipcode, A.bedrooms, A.price as airbnb_price_day, B.price as zillow_price_month
FROM (SELECT DATE_TRUNC( date , MONTH ) as date, zipcode, 
	case when bedrooms >= 5 then 5 end as bedrooms, room_type,
	case when c.price is null then percentile_cont(l.price, 0.5) over (partition by date) 
	else percentile_cont(c.price, 0.5) over (partition by date) 
	end as price
FROM `kapetanakissaenzcs327e.boston_temp.Calendar`  c join `kapetanakissaenzcs327e.boston_temp.Listing`  l on c.listing_id = l.id
WHERE bedrooms >= 5 AND room_type = 'Entire home/apt' AND zipcode is not null)as A
Join `kapetanakissaenzcs327e.zillow.Rental_Price_5BedroomOrMore`  as B on A.zipcode = B.zipcode AND A.date = B.date where B.price is not null)
Order By airbnb_price_day

#Create Boston v_Revenue_Crossover
#Calculate the crossover_pt with all the data from previous
SELECT date, zipcode, bedrooms, airbnb_price_day, zillow_price_month, (zillow_price_month/airbnb_price_day) AS crossover_pt
FROM `kapetanakissaenzcs327e.boston_temp.temp_table`