UPDATE Host SET response_rate = REPLACE(price, '%', '');
UPDATE Host SET response_rate = REPLACE(price, 'N/A', NULL);
ALTER TABLE Host ALTER COLUMN response_rate numeric;

ALTER TABLE Host ADD COLUMN city varchar(50), 
ADD COLUMN state varchar(50), 
ADD COLUMN country varchar(50);

UPDATE Host SET city = split_part(location, ',', 1), 
state = split_part(location, ',', 2), 
country = split_part(location, ',', 3);

ALTER TABLE Host DROP COLUMN location;

