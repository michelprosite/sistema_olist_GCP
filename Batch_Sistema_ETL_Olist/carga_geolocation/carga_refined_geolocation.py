from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import apache_beam as beam
import pyarrow.parquet as pq
import csv
import sys
import os

# importa a pasta funcao
current_dir = os.path.dirname(os.path.abspath(__file__))
funcoes_dir = os.path.join(current_dir, '..', 'funcoes')
sys.path.append(funcoes_dir)

from funcoes import registraDataLog

# Define o nome das colunas da tabela de destino no BigQuery
column_names = ["geolocation_zip_code_prefix:INTEGER, geolocation_lat:FLOAT, geolocation_lng:FLOAT, geolocation_city:STRING, geolocation_state:STRING"]

# Define o schema da tabela de destino no BigQuery
schema = ','.join(column_names)

def save_to_bigquery(data):
    return data | 'Write to BigQuery' >> WriteToBigQuery(
        table='olist-brasil-project.olist_brasil.geolocation',
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method="STREAMING_INSERTS"
    )

def run_pipeline():
    # Define as opções do pipeline
    options = PipelineOptions()


class FormatOutputFnGeolocation(beam.DoFn):
    def process(self, row):
        formatted_row = {
            "geolocation_zip_code_prefix": int(row[0]),
            "geolocation_lat": float(row[1]),
            "geolocation_lng": float(row[2]),
            "geolocation_city": str(row[3]),
            "geolocation_state": str(row[4])
        }
        yield formatted_row

class ToList(beam.DoFn):
    lista = []

    def process(self, element):
        self.lista.append(element)

def unpack_element(element):
    key, values = element
    return (key, *values)

def carga_refined_geolocation():
    # Define o pipeline
    with beam.Pipeline() as pipeline:
        # Read the Parquet file
        parquet_data = (
            pipeline
            | 'Read Parquet' >> beam.Create([None])
            | 'Load Parquet' >> beam.Map(lambda x: pq.read_table('gs://olist_brasil_project/trusted/geolocation.parquet'))
            | 'Extract Rows' >> beam.FlatMap(lambda table: table.to_pandas().to_dict('records'))
        )

        # Process the Parquet data and convert to key-value pairs
        processed_data = (
            parquet_data
            | 'Format Output' >> beam.ParDo(FormatOutputFnGeolocation())
        )

        sorted_data = (
            processed_data
            | 'Sort by geolocation_zip_code_prefix' >> beam.Map(lambda row: (row['geolocation_zip_code_prefix'], row))
            | 'Group by geolocation_zip_code_prefix' >> beam.GroupByKey()
            | 'Combine per key' >> beam.CombinePerKey(lambda rows: list(rows)[0])
            | 'Desmpacota os elementos para dict' >> beam.ParDo(unpack_element)
            #| beam.ParDo(ToList())
            #| beam.Map(print)
        )

# Save the processed data to BigQuery
        save_to_bigquery(processed_data)

        print(f'{registraDataLog()} - Processo de carga BigQuery geolocation - OK')

if __name__ == '__main__':
    run_pipeline()


