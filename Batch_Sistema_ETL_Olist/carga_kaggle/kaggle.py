import os
import glob
from google.cloud import storage
import subprocess
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime

def cargaKaggle():
    # Configurações
    bucket_name = 'olist_brasil_project'
    bucket_folder = 'tranzient'
    local_folder = '03_carga_kaggle_csv'
    batch_size = 1000  # Tamanho do lote de arquivos a serem movidos

    # Criar diretório kaggle_csv
    subprocess.run(["mkdir", "-p", local_folder])

    # Baixar o conjunto de dados do Kaggle
    subprocess.run(["kaggle", "datasets", "download", "-d", "olistbr/brazilian-ecommerce"])

    # Mover o arquivo zip para o diretório kaggle_csv (com substituição)
    subprocess.run(["mv", "-f", "brazilian-ecommerce.zip", local_folder])

    # Extrair o arquivo zip para o diretório kaggle_csv (com substituição)
    subprocess.run(["unzip", "-o", f"{local_folder}/brazilian-ecommerce.zip", "-d", local_folder])

    # Excluir o arquivo zip
    os.remove(f"{local_folder}/brazilian-ecommerce.zip")

    # Criar cliente do Google Cloud Storage
    storage_client = storage.Client()

    # Listar todos os arquivos no diretório local
    files = glob.glob(os.path.join(local_folder, '*'))

    # Mover cada arquivo para o bucket do Google Cloud Storage em lotes
    for i in range(0, len(files), batch_size):
        batch_files = files[i:i+batch_size]

        # Mover cada arquivo do lote para o bucket
        for file in batch_files:
            # Obter o nome do arquivo sem o caminho do diretório local
            filename = os.path.basename(file)

            # Caminho de destino no Google Cloud Storage
            destination_blob_name = f'{bucket_folder}/{filename}'

            # Enviar arquivo para o bucket, sobrescrevendo se já existir
            blob = storage_client.bucket(bucket_name).blob(destination_blob_name)
            blob.upload_from_filename(file, if_generation_match=blob.generation)

            print(f"Arquivo {filename} movido para gs://{bucket_name}/{destination_blob_name}")

        # Excluir arquivos locais do lote após mover para o bucket
        for file in batch_files:
            os.remove(file)

            print(f"Arquivo {file} excluído")

    # Excluir diretório local vazio
    os.rmdir(local_folder)




