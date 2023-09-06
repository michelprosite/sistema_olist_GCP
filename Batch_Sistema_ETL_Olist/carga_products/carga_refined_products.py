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
column_names = ["product_id:STRING, product_category_name:STRING, product_name_lenght:FLOAT, product_description_lenght:FLOAT, product_photos_qty:INTEGER, product_weight_g:FLOAT, product_length_cm:FLOAT, product_height_cm:FLOAT, product_width_cm:FLOAT"]

# Define o schema da tabela de destino no BigQuery
schema = ','.join(column_names)

def save_to_bigquery(data):
    return data | 'Write to BigQuery' >> WriteToBigQuery(
        table='olist-brasil-project.olist_brasil.products',
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method="STREAMING_INSERTS"
    )

def run_pipeline():
    # Define as opções do pipeline
    options = PipelineOptions()


class FormatOutputFnProducts(beam.DoFn):
    def process(self, row):
        formatted_row = {
            "product_id": str(row[0]),
            "product_category_name": str(row[1]),
            "product_name_lenght": float(row[2]),
            "product_description_lenght": float(row[3]),
            "product_photos_qty": int(row[4]),
            "product_weight_g": float(row[5]),
            "product_length_cm": float(row[6]),
            "product_height_cm": float(row[7]),
            "product_width_cm": float(row[8])
        }
        yield formatted_row

class ToList(beam.DoFn):
    lista = []

    def process(self, element):
        self.lista.append(element)

def unpack_element(element):
    key, values = element
    return (key, *values)

def carga_refined_products():
    # Define o pipeline
    with beam.Pipeline() as pipeline:
        # Read the Parquet file
        parquet_data = (
            pipeline
            | 'Read Parquet' >> beam.Create([None])
            | 'Load Parquet' >> beam.Map(lambda x: pq.read_table('gs://olist_brasil_project/trusted/products.parquet'))
            | 'Extract Rows' >> beam.FlatMap(lambda table: table.to_pandas().to_dict('records'))
        )

        # Process the Parquet data and convert to key-value pairs
        processed_data = (
            parquet_data
            | 'Format Output' >> beam.ParDo(FormatOutputFnProducts())
        )

        sorted_data = (
            processed_data
            | 'Sort by product_id' >> beam.Map(lambda row: (row['product_id'], row))
            | 'Group by product_id' >> beam.GroupByKey()
            | 'Combine per key' >> beam.CombinePerKey(lambda rows: list(rows)[0])
            | 'Desmpacota os elementos para dict' >> beam.ParDo(unpack_element)
            #| beam.ParDo(ToList())
            #| beam.Map(print)
        )

# Save the processed data to BigQuery
        save_to_bigquery(processed_data)

        print(f'{registraDataLog()} - Processo de carga BigQuery products - OK')

if __name__ == '__main__':
    run_pipeline()


