ALTER TABLE Calendar ADD CONSTRAINT fk_calendar_listings FOREIGN KEY (listing_id) REFERENCES listings (id);
ALTER TABLE Listings ADD CONSTRAINT fk_listings_neighbourhoods FOREIGN KEY (neighbourhood_cleansed) REFERENCES neighbourhoods (neighbourhood);
ALTER TABLE Reviews ADD CONSTRAINT fk_reviews_listings FOREIGN KEY (listing_id) REFERENCES listings (id);
ALTER TABLE summary_listings ADD CONSTRAINT fk_sumlists_neighbourhoods FOREIGN KEY (neighbourhood) REFERENCES neighbourhoods (neighbourhood);
ALTER TABLE summary_reviews ADD CONSTRAINT fk_sumrevs_listings FOREIGN KEY (listing_id) REFERENCES listings (id);