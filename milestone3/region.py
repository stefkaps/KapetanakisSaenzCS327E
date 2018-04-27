from __future__ import absolute_import

import argparse
import logging
import warnings
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

parsed_records = []

def init_bigquery_table():
	table_schema = bigquery.TableSchema()

	zipcode_field = bigquery.TableFieldSchema()
	zipcode_field.name = 'zipcode'
	zipcode_field.type = 'integer'
	zipcode_field.mode = 'required'
	table_schema.fields.append(zipcode_field)
	
	city_field = bigquery.TableFieldSchema()
	city_field.name = 'city'
	city_field.type = 'string'
	city_field.mode = 'nullable'
	table_schema.fields.append(city_field)
	
	state_field = bigquery.TableFieldSchema()
	state_field.name = 'state'
	state_field.type = 'string'
	state_field.mode = 'nullable'
	table_schema.fields.append(state_field)

	metro_field = bigquery.TableFieldSchema()
	metro_field.name = 'metro'
	metro_field.type = 'string'
	metro_field.mode = 'nullable'
	table_schema.fields.append(metro_field)

	county_field = bigquery.TableFieldSchema()
	county_field.name = 'county'
	county_field.type = 'string'
	county_field.mode = 'nullable'
	table_schema.fields.append(county_field)
	
	return table_schema;
	
def create_bigquery_record(tuple):

	# tuple format = (zipcode, date, price)
	# For example, (78705, '2015-01-01', 100.0)
	# Note: price is an optional field
		
	zipcode, city, state, metro, county = tuple
	bq_record = {'zipcode': zipcode, 'city': city, 'state': state, 'metro': metro, 'county': county}
	
	return bq_record


def parse_line(line): 
	
	temp = []

	tokens = line.split(",")

	zipcode_with_quotes = tokens[0]
        city = tokens[1]
	state = tokens[2]
	metro = tokens[3]
	county = tokens[4]
	zipcode = int(zipcode_with_quotes.strip('"'));


	temp.append((zipcode, city, state, metro, county))
	if not set(temp).issubset(parsed_records):
		parsed_records.append((zipcode, city, state, metro, county))	
	
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
      '--job_name=zillow-rentals-region' # assign descriptive name to this job, all in lower case letters
	])
	
	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = True # save_main_session provides global context
	
	with beam.Pipeline(options=pipeline_options) as p:
	
		table_name = "kapetanakissaenzcs327e:zillow.Region" # format: project_id:dataset.table
		table_schema = init_bigquery_table()
    
		line1 = p | 'ReadFile' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_1Bedroom.csv')
		line2 = p | 'ReadFile2' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_2Bedroom.csv')
		line3 = p | 'ReadFile3' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_3Bedroom.csv')
		line4 = p | 'ReadFile4' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_4Bedroom.csv')
		line5 = p | 'ReadFile5' >> beam.io.ReadFromText('gs://kapetanakissaenzcs327e1/zillow/Zip_MedianRentalPrice_5BedroomOrMore.csv')
		
		lines = ((line1,line2, line3, line4, line5) | 'CombineFile' >> beam.Flatten())
	
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
