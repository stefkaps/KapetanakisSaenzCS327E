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