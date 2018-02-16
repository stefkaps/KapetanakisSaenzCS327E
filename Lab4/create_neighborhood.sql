create table Neighborhood as(
   SELECT DISTINCT neighbourhood, zipcode FROM Listings
);

ALTER TABLE Neighborhood RENAME COLUMN neighbourhood TO neighborhood_name;

DELETE FROM Neighborhood WHERE neighborhood_name IS NULL;
DELETE FROM Neighborhood WHERE zipcode IS NULL;

ALTER TABLE Neighborhood ADD PRIMARY KEY (neighbourhood_name, zipcode);

DROP TABLE Neighbourhoods;
