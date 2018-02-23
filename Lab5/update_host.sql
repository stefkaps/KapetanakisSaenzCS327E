UPDATE Host SET response_rate = REPLACE(price, '%', '');
UPDATE Host SET response_rate = REPLACE(price, 'N/A', NULL);
ALTER TABLE Host ALTER COLUMN response_rate numeric;

UPDATE Host SET city = split_part(location, ',', 1), state = split_part(location, ',', 2), country = split_part(location, ',', 3);

ALTER TABLE Host DROP COLUMN location;

