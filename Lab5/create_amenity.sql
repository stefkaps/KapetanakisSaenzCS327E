create table Amenity as(
   SELECT DISTINCT listing_id, amenities FROM Listing
);

ALTER TABLE Amenity ADD COLUMN amenity_name varchar(50);

ALTER TABLE Amenity SET amenity_name = regexp_split_to_table(amenities, ',');
ALTER TABLE Amenity DROP COLUMN amenities;

ALTER TABLE Amenity ADD PRIMARY KEY (listing_id, amenity_name);

ALTER TABLE Amenity ADD FOREIGN KEY (listing_id) REFERENCES Listing;

ALTER TABLE Listing DROP COLUMN amenities;
