# Fourth Beam Job
# To run: python beam4.py
#

from __future__ import absolute_import

import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

def init_bigquery_table():
	table_schema = bigquery.TableSchema()

	zipcode_field = bigquery.TableFieldSchema()
	zipcode_field.name = 'zipcode'
	zipcode_field.type = 'integer'
	zipcode_field.mode = 'required'
	table_schema.fields.append(zipcode_field)
	
	date_field = bigquery.TableFieldSchema()
	date_field.name = 'date'
	date_field.type = 'date'
	date_field.mode = 'required'
	table_schema.fields.append(date_field)
	
	price_field = bigquery.TableFieldSchema()
	price_field.name = 'price'
	price_field.type = 'float'
	price_field.mode = 'nullable'
	table_schema.fields.append(price_field)
	
	return table_schema;
	
def create_bigquery_record(tuple):

	# tuple format = (zipcode, date, price)
	# For example, (78705, '2015-01-01', 100.0)
	# Note: price is an optional field
		
	zipcode, date, price = tuple
	bq_record = {'zipcode': zipcode, 'date': date, 'price': price}
	
	return bq_record

def convert_price(price):
	if len(price) > 0:
		price = float(price)
	else:
		price = None
	return price
	
def convert_month(month):
	month_len = len(str(month))
	if month_len == 1:
		month_str = "0" + str(month)
	else:
		month_str = str(month)
	return month_str

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
	
	
def parse_records(records):
	return records
	
		
def run(argv=None):	
	
	with beam.Pipeline(options=PipelineOptions()) as p:
	
		table_name = "kapetanakissaenzcs327e:zillow.Rental_Price_1Bedroom" # format: project_id:dataset.table
		table_schema = init_bigquery_table()
    
		lines = p | 'ReadFile' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_1Bedroom.csv')
	
		list_records = lines | 'CreateListRecords' >> (beam.Map(parse_line))
        
		list_records | 'WriteTmpFile1' >> beam.io.WriteToText('/home/rsaenz8080/code/tuple_records', file_name_suffix='.txt')
		
		tuple_records = list_records | 'CreateTupleRecords' >> (beam.FlatMap(parse_records))
		
		tuple_records | 'WriteTmpFile2' >> beam.io.WriteToText('/home/shirley_cohen/code/tmp/tuple_records', file_name_suffix='.txt')
	
		bigquery_records = tuple_records | 'CreateBigQueryRecord' >> beam.Map(create_bigquery_record)
	
		bigquery_records | 'WriteTmpFile3' >> beam.io.WriteToText('/home/rsaenz8080/code/bq_records', file_name_suffix='.txt')
	
		bigquery_records | 'WriteBigQuery' >> beam.io.Write(
		    beam.io.BigQuerySink(
		        table_name,
		        schema = table_schema,
		        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
		        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()