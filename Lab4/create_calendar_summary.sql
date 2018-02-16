create table Calendar_summary as(SELECT id, calendar_updated, calendar_last_scraped,
availability_30, availability_60, availability_90 FROM Listings);

ALTER TABLE Calendar_summary RENAME COLUMN id TO listing_id;
ALTER TABLE Calendar_summary RENAME COLUMN calendar_last_scraped TO from_date;

ALTER TABLE Calendar_summary ADD PRIMARY KEY (listing_id,from_date);

ALTER TABLE Calendar_summary ADD FOREIGN KEY (listing_id) REFERENCES Listings;