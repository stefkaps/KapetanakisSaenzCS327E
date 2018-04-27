# Fifth Beam Job
# To run: python beam5.py
#

from __future__ import absolute_import

import argparse
import logging
import warnings
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
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
	
	start_month_index = 43; # 2015-01 is on column 58
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
	
	parser = argparse.ArgumentParser()
	known_args, pipeline_args = parser.parse_known_args(argv)
	pipeline_args.extend([	
      '--runner=DataflowRunner', # use DataflowRunner to run on Dataflow or DirectRunner to run on local VM
      '--project=kapetanakissaenzcs327e', # change to your project_id
      '--staging_location=gs://kapetanakissaenzcs327e1/staging', # change to your bucket
      '--temp_location=gs://kapetanakissaenzcs327e1/temp', # change to your bucket
      '--job_name=zillow-rentals-5bedroomormore' # assign descriptive name to this job, all in lower case letters
	])
	
	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True # save_main_session provides global context
	
	with beam.Pipeline(options=pipeline_options) as p:
	
		table_name = "kapetanakissaenzcs327e:zillow.Rental_Price_5BedroomOrMore" # format: project_id:dataset.table
		table_schema = init_bigquery_table()
    
		lines = p | 'ReadFile' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_5BedroomOrMore.csv')
	
		list_records = lines | 'CreateListRecords' >> (beam.Map(parse_line))
        
		list_records | 'WriteTmpFile1' >> beam.io.WriteToText('gs://kapetanakissaenzcs327e1/temp/list_records', file_name_suffix='.txt')
	
		tuple_records = list_records | 'CreateTupleRecords' >> (beam.FlatMap(parse_records))
		
		tuple_records | 'WriteTmpFile2' >> beam.io.WriteToText('gs://kapetanakissaenzcs327e1/temp/tuple_records', file_name_suffix='.txt')

		bigquery_records = tuple_records | 'CreateBigQueryRecord' >> beam.Map(create_bigquery_record)
	
		bigquery_records | 'WriteTmpFile3' >> beam.io.WriteToText('gs://kapetanakissaenzcs327e1/temp/bq_records', file_name_suffix='.txt')
	
		bigquery_records | 'WriteBigQuery' >> beam.io.Write(
		    beam.io.BigQuerySink(
		        table_name,
		        schema = table_schema,
		        create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
		        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE))

if __name__ == '__main__':
	warnings.filterwarnings("ignore")
	logging.getLogger().setLevel(logging.DEBUG) # change to INFO or ERROR for less verbose logging
	run()
