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
column_names = ["order_id:STRING, customer_id:STRING, order_status:STRING, order_purchase_timestamp:DATE, order_approved_at:DATE, order_delivered_carrier_date:DATE, order_delivered_customer_date:DATE, order_estimated_delivery_date:DATE"]

# Define o schema da tabela de destino no BigQuery
schema = ','.join(column_names)

def save_to_bigquery(data):
    return data | 'Write to BigQuery' >> WriteToBigQuery(
        table='olist-brasil-project.olist_brasil.orders',
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method="STREAMING_INSERTS"
    )

def run_pipeline():
    # Define as opções do pipeline
    options = PipelineOptions()


import datetime
import apache_beam as beam

class FormatOutputFnOrders(beam.DoFn):
    def process(self, row):
        formatted_row = {
            "order_id": str(row[0]),
            "customer_id": str(row[1]),
            "order_status": str(row[2]),
            "order_purchase_timestamp": datetime.datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S").date() if row[3] and row[3] != 'None' else None,
            "order_approved_at": datetime.datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S").date() if row[4] and row[4] != 'None' else None,
            "order_delivered_carrier_date": datetime.datetime.strptime(row[5], "%Y-%m-%d %H:%M:%S").date() if row[5] and row[5] != 'None' else None,
            "order_delivered_customer_date": datetime.datetime.strptime(row[6], "%Y-%m-%d %H:%M:%S").date() if row[6] and row[6] != 'None' else None,
            "order_estimated_delivery_date": datetime.datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S").date() if row[7] and row[7] != 'None' else None,
        }
        yield formatted_row


class ToList(beam.DoFn):
    lista = []

    def process(self, element):
        self.lista.append(element)

def unpack_element(element):
    key, values = element
    return (key, *values)

def carga_refined_orders():
    # Define o pipeline
    with beam.Pipeline() as pipeline:
        # Read the Parquet file
        parquet_data = (
            pipeline
            | 'Read Parquet' >> beam.Create([None])
            | 'Load Parquet' >> beam.Map(lambda x: pq.read_table('gs://olist_brasil_project/trusted/orders.parquet'))
            | 'Extract Rows' >> beam.FlatMap(lambda table: table.to_pandas().to_dict('records'))
        )

        # Process the Parquet data and convert to key-value pairs
        processed_data = (
            parquet_data
            | 'Format Output' >> beam.ParDo(FormatOutputFnOrders())
        )

        sorted_data = (
            processed_data
            | 'Sort by order_id' >> beam.Map(lambda row: (row['order_id'], row))
            | 'Group by order_id' >> beam.GroupByKey()
            | 'Combine per key' >> beam.CombinePerKey(lambda rows: list(rows)[0])
            | 'Desmpacota os elementos para dict' >> beam.ParDo(unpack_element)
            #| beam.ParDo(ToList())
            #| beam.Map(print)
        )

# Save the processed data to BigQuery
        save_to_bigquery(processed_data)

        print(f'{registraDataLog()} - Processo de carga BigQuery orders - OK')

if __name__ == '__main__':
    run_pipeline()


