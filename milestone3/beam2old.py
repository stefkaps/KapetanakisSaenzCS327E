# Second Beam Job
# To run: python beam2.py
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

def parse_records(line):

	tokens = line.split(",")
	zipcode_with_quotes = tokens[0]
	zipcode = int(zipcode_with_quotes.strip('"'));
		
	start_index = 58;
	end_index = start_index + 12;
	
	year = 2015;
	month = 1;
	day = "01"	
	price = 0.0
		
	for i in range(start_index, end_index):
		price = tokens[i]
		date = str(year) + "-" + convert_month(month) + "-" + day
		return (zipcode, date, price)
		month += 1
		
	
with beam.Pipeline(options=PipelineOptions()) as p:
    
	lines = p | 'ReadFile' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_1Bedroom.csv')
	
	tuple_records = lines | 'SplitLines' >> (beam.Map(parse_records))
        
	tuple_records | 'WriteFile' >> beam.io.WriteToText('/home/rsaenz8080/code/tuple_records', file_name_suffix='.txt')
