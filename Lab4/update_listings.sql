ALTER TABLE Listings DROP COLUMN host_url,DROP COLUMN host_name,DROP COLUMN host_since,DROP COLUMN host_location,DROP COLUMN
host_about,DROP COLUMN host_response_time,DROP COLUMN host_response_rate,DROP COLUMN
host_acceptance_rate,DROP COLUMN host_is_superhost,DROP COLUMN host_thumbnail_url,DROP COLUMN
host_picture_url,DROP COLUMN host_neighbourhood,DROP COLUMN host_listings_count,DROP COLUMN
host_total_listings_count,DROP COLUMN host_verifications,DROP COLUMN
host_has_profile_pic,DROP COLUMN host_identity_verified,DROP COLUMN
calculated_host_listings_count;

ALTER TABLE Listings ADD FOREIGN KEY (host_id) REFERENCES Host;

ALTER TABLE Listings DROP COLUMN calendar_updated, DROP COLUMN calendar_last_scraped, DROP COLUMN
availability_30, DROP COLUMN availability_60, DROP COLUMN availability_90;

ALTER TABLE Listings DROP COLUMN neighbourhood_cleansed, DROP COLUMN neighbourhood_group_cleansed;

ALTER TABLE Listings RENAME COLUMN neighbourhood TO neighborhood;

ALTER TABLE Summary_Listings DROP COLUMN neighbourhood_group;

ALTER TABLE Listings ADD FOREIGN KEY (neighborhood, zipcode) REFERENCES Neighborhood;

ALTER TABLE Summary_Listings RENAME COLUMN neighbourhood TO zipcode;

UPDATE Listings SET street = split_part(street, ',', 1)

UPDATE Listings SET price = REPLACE(price, ',', '');
UPDATE Listings SET price = REPLACE(price, '$', '');
UPDATE Listings SET weekly_price = REPLACE(weekly_price, ',', '');
UPDATE Listings SET weekly_price = REPLACE(weekly_price, '$', '');
UPDATE Listings SET monthly_price = REPLACE(monthly_price, ',', '');
UPDATE Listings SET monthly_price = REPLACE(monthly_price, '$', '');
UPDATE Listings SET security_deposit = REPLACE(security_deposit, ',', '');
UPDATE Listings SET security_deposit = REPLACE(security_deposit, '$', '');
UPDATE Listings SET cleaning_fee = REPLACE(cleaning_fee, ',', '');
UPDATE Listings SET cleaning_fee = REPLACE(cleaning_fee, '$', '');

ALTER TABLE Listings ALTER COLUMN  price numeric;
ALTER TABLE Listings ALTER COLUMN  weekly_price numeric;
ALTER TABLE Listings ALTER COLUMN  monthly_price numeric;
ALTER TABLE Listings ALTER COLUMN  cleaning_fee numeric;

ALTER TABLE Listings RENAME TO Listing;
ALTER TABLE Summary_Listings RENAME TO Summary_Listing;
