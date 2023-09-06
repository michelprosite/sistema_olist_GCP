from apache_beam.io import parquetio
from apache_beam.io.gcp import gcsio
import pyarrow.parquet as pq
import apache_beam as beam
import pyarrow as pa
import pandas as pd
import sys
import os


# importa a pasta funcao
current_dir = os.path.dirname(os.path.abspath(__file__))
funcoes_dir = os.path.join(current_dir, '..', 'funcoes')
sys.path.append(funcoes_dir)

from funcoes import parse_csv_payments
from funcoes import registraDataLog

class ToList(beam.DoFn):
    lista = []

    def process(self, element):
        self.lista.append(element)


def carga_raw_payments():
    # Define o pipeline
    with beam.Pipeline() as pipeline:
        # Read the CSV file
        csv_lines = (
            pipeline
            | 'Read CSV' >> beam.io.ReadFromText('gs://olist_brasil_project/tranzient/olist_order_payments_dataset.csv', skip_header_lines=1)
        )
        
        # Process the CSV data (you can modify this step according to your needs)
        processed_data = (
            csv_lines
            | 'Parse CSV' >> beam.Map(parse_csv_payments)
            | "Format Output payments" >> beam.Map(lambda element: list(element.values()))
            | beam.ParDo(ToList())
        )

        # Executar o pipeline
        result = pipeline.run()
        result.wait_until_finish()

        # Obter a lista de resultados
        output_data = ToList.lista

        # Converter a lista em um DataFrame pandas
        df = pd.DataFrame(output_data)

        # Converter o DataFrame em uma tabela do pyarrow
        table = pa.Table.from_pandas(df)

        # Escrever a tabela em um arquivo Parquet
        output_path = 'gs://olist_brasil_project/raw/up_payments.parquet'
        pq.write_table(table, output_path)


    with beam.Pipeline() as pipeline:
        # Leia o primeiro arquivo Parquet
        input_parquet_file = 'gs://olist_brasil_project/raw/payments.parquet'
        file_exists = gcsio.GcsIO().exists(input_parquet_file)
        if file_exists:
            data1 = pipeline | 'Read Parquet 1' >> parquetio.ReadFromParquet('gs://olist_brasil_project/raw/payments.parquet')
            data2 = pipeline | 'Read Parquet 2' >> parquetio.ReadFromParquet('gs://olist_brasil_project/raw/up_payments.parquet')
        else:
            data1 = pipeline | 'Read Parquet 2' >> parquetio.ReadFromParquet('gs://olist_brasil_project/raw/up_payments.parquet')
            data2 = data1
        # Leia o segundo arquivo Parquet
        

        # Realize a concatenação (merge) dos dados
        merged_data = (
            (data1, data2)
            | beam.Flatten()  # Aplicar a função de processamento nos dados
            | beam.Map(lambda element: ','.join(str(value ) for value in element.values()))
        )

        processed_data_2 = (
            merged_data
            | 'Parse CSV' >> beam.Map(parse_csv_payments)
            | "Format Output payments" >> beam.Map(lambda element: list(element.values()))
            | beam.ParDo(ToList())
        )

        # Executar o pipeline
        result = pipeline.run()
        result.wait_until_finish()

        # Obter a lista de resultados
        output_data = ToList.lista

        # Converter a lista em um DataFrame pandas
        df = pd.DataFrame(output_data)
        df = df.sort_values(by=[0])
        df = df.drop_duplicates(subset=0, keep='last')

        # Converter o DataFrame em uma tabela do pyarrow
        table = pa.Table.from_pandas(df)

        # Escrever a tabela em um arquivo Parquet
        output_path = 'gs://olist_brasil_project/raw/payments.parquet'
        pq.write_table(table, output_path)



        # Defina o caminho completo para o arquivo Parquet no Cloud Storage
        parquet_file = 'gs://olist_brasil_project/raw/up_payments.parquet'

        # Crie uma instância da classe GcsIO
        gcs = gcsio.GcsIO()

        # Exclua o arquivo Parquet
        gcs.delete(parquet_file)

        print(f'{registraDataLog()} - Processo de carga RAW payments - OK')
        

