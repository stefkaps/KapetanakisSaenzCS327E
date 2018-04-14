# Third Beam Job
# To run: python beam3.py
#

from __future__ import absolute_import

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def convert_month(month):
	month_len = len(str(month))
	if month_len == 1:
		month_str = "0" + str(month)
	else:
		month_str = str(month)
	return month_str

def convert_price(price):
	if len(price) > 0:
		price = float(price)
	else:
		price = None
	return price

def parse_line(line):
  
	parsed_records = []

	tokens = line.split(",")
	zipcode_with_quotes = tokens[0]
	zipcode = int(zipcode_with_quotes.strip('"'));
	
	start_year = 2015
	end_year = 2019
	
	start_month_index = 58; # 2015-01 is on column 58
	end_month_index = start_month_index + 12;
	
	day = "01"	
	price = 0.0
	
	for year in range(start_year, end_year):
	
		month = 1;
		
		for month_index in range(start_month_index, end_month_index):
			
			price = tokens[month_index]
			price = convert_price(price)
				
			date = str(year) + "-" + convert_month(month) + "-" + day
			
			parsed_records.append((zipcode, date, price))
			
			month += 1 # increments up to 12 for years 2015-2017
			
			# only go through the loop once for 2018
			if (year == 2018): break
		
		start_month_index = end_month_index
		end_month_index = start_month_index + 12
	
	return parsed_records

	
with beam.Pipeline(options=PipelineOptions()) as p:
    
	lines = p | 'ReadFile' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_1Bedroom.csv')
	
	list_records = lines | 'CreateListRecords' >> (beam.Map(parse_line))
        
	list_records | 'WriteFile' >> beam.io.WriteToText('/home/rsaenz8080/code/tuple_records', file_name_suffix='.txt')
