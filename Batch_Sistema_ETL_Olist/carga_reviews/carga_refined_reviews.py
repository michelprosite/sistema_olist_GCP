from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import apache_beam as beam
import pyarrow.parquet as pq
import datetime
import csv
import sys
import os

# importa a pasta funcao
current_dir = os.path.dirname(os.path.abspath(__file__))
funcoes_dir = os.path.join(current_dir, '..', 'funcoes')
sys.path.append(funcoes_dir)

from funcoes import registraDataLog

# Define o nome das colunas da tabela de destino no BigQuery
column_names = ["review_id:STRING, order_id:STRING, review_score:INTEGER, review_comment_title:STRING, review_comment_message:STRING, review_creation_date:DATE, review_answer_timestamp:DATE"]

# Define o schema da tabela de destino no BigQuery
schema = ','.join(column_names)

def save_to_bigquery(data):
    return data | 'Write to BigQuery' >> WriteToBigQuery(
        table='olist-brasil-project.olist_brasil.reviews',
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method="STREAMING_INSERTS"
    )

def run_pipeline():
    # Define as opções do pipeline
    options = PipelineOptions()


class FormatOutputFnReviews(beam.DoFn):
    def process(self, row):
        review_creation_date = None
        review_answer_timestamp = None

        if row[5] and row[5] != 'None':
            try:
                review_creation_date = datetime.datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                pass

        if row[6] and row[6] != 'None':
            try:
                review_answer_timestamp = datetime.datetime.strptime(row[6], "%Y-%m-%d %H:%M:%S").date()
            except ValueError:
                pass

        formatted_row = {
            "review_id": str(row[0]),
            "order_id": str(row[1]),
            "review_score": int(row[2]),
            "review_comment_title": str(row[3]),
            "review_comment_message": str(row[4]),
            "review_creation_date": review_creation_date,
            "review_answer_timestamp": review_answer_timestamp,
        }
        yield formatted_row

class ToList(beam.DoFn):
    lista = []

    def process(self, element):
        self.lista.append(element)

def unpack_element(element):
    key, values = element
    return (key, *values)

def carga_refined_reviews():
    # Define o pipeline
    with beam.Pipeline() as pipeline:
        # Read the Parquet file
        parquet_data = (
            pipeline
            | 'Read Parquet' >> beam.Create([None])
            | 'Load Parquet' >> beam.Map(lambda x: pq.read_table('gs://olist_brasil_project/trusted/reviews.parquet'))
            | 'Extract Rows' >> beam.FlatMap(lambda table: table.to_pandas().to_dict('records'))
        )

        # Process the Parquet data and convert to key-value pairs
        processed_data = (
            parquet_data
            | 'Format Output' >> beam.ParDo(FormatOutputFnReviews())
        )

        sorted_data = (
            processed_data
            | 'Sort by review_id' >> beam.Map(lambda row: (row['review_id'], row))
            | 'Group by review_id' >> beam.GroupByKey()
            | 'Combine per key' >> beam.CombinePerKey(lambda rows: list(rows)[0])
            | 'Desmpacota os elementos para dict' >> beam.ParDo(unpack_element)
            #| beam.ParDo(ToList())
            #| beam.Map(print)
        )

# Save the processed data to BigQuery
        save_to_bigquery(processed_data)

        print(f'{registraDataLog()} - Processo de carga BigQuery reviews - OK')

if __name__ == '__main__':
    run_pipeline()


