create table Host as(
   SELECT DISTINCT host_id, host_url, host_name, host_since, host_location,
host_about, host_response_time, host_response_rate,
host_acceptance_rate, host_is_superhost, host_thumbnail_url,
host_picture_url, host_neighbourhood, host_listings_count,
host_total_listings_count, host_verifications,
host_has_profile_pic, host_identity_verified,
calculated_host_listings_count FROM Listings
);

ALTER TABLE Host RENAME COLUMN host_id TO id;
ALTER TABLE Host RENAME COLUMN host_url TO url;
ALTER TABLE Host RENAME COLUMN host_name TO name;
ALTER TABLE Host RENAME COLUMN host_since TO since;
ALTER TABLE Host RENAME COLUMN host_location TO location;
ALTER TABLE Host RENAME COLUMN host_about TO about;
ALTER TABLE Host RENAME COLUMN host_response_time TO response_time;
ALTER TABLE Host RENAME COLUMN host_response_rate TO response_rate;
ALTER TABLE Host RENAME COLUMN host_acceptance_rate TO acceptance_rate;
ALTER TABLE Host RENAME COLUMN host_is_superhost TO is_superhost;
ALTER TABLE Host RENAME COLUMN host_thumbnail_url TO thumbnail_url;
ALTER TABLE Host RENAME COLUMN host_picture_url TO picture_url;
ALTER TABLE Host RENAME COLUMN host_neighbourhood TO neighbourhood;
ALTER TABLE Host RENAME COLUMN host_listings_count TO listings_count;
ALTER TABLE Host RENAME COLUMN host_total_listings_count TO total_istings_count;
ALTER TABLE Host RENAME COLUMN host_verifications TO verifications;
ALTER TABLE Host RENAME COLUMN host_has_profile_pic TO has_profile_pic;
ALTER TABLE Host RENAME COLUMN host_identity_verified TO identity_verified;

ALTER TABLE Host ADD PRIMARY KEY (id);
