from apache_beam.options.pipeline_options import PipelineOptions
import os
import sys
import apache_beam as beam
import datetime
import schedule
import time
import logging
import glob
from google.cloud import storage
import subprocess

# Importando função da carga dos Arquivos do Kaggle
from carga_kaggle.kaggle import cargaKaggle

# Importando os pipelines custmers
from carga_customers.carga_raw_customers import carga_raw_customers
from carga_customers.carga_trusted_customers import carga_trusted_customers
from carga_customers.carga_refined_customers import carga_refined_customers

# Importando os pipelines geolocation
from carga_geolocation.carga_raw_geolocation import carga_raw_geolocation
from carga_geolocation.carga_trusted_geolocation import carga_trusted_geolocation
from carga_geolocation.carga_refined_geolocation import carga_refined_geolocation

# Importando os pipelines items
from carga_items.carga_raw_items import carga_raw_items
from carga_items.carga_trusted_items import carga_trusted_items
from carga_items.carga_refined_items import carga_refined_items

# Importando os pipelines orders
from carga_orders.carga_raw_orders import carga_raw_orders
from carga_orders.carga_trusted_orders import carga_trusted_orders
from carga_orders.carga_refined_orders import carga_refined_orders

# Importando os pipelines payments
from carga_payments.carga_raw_payments import carga_raw_payments
from carga_payments.carga_trusted_payments import carga_trusted_payments
from carga_payments.carga_refined_payments import carga_refined_payments

# Importando os pipelines products
from carga_products.carga_raw_products import carga_raw_products
from carga_products.carga_trusted_products import carga_trusted_products
from carga_products.carga_refined_products import carga_refined_products

# Importando os pipelines reviews
from carga_reviews.carga_raw_reviews import carga_raw_reviews
from carga_reviews.carga_trusted_reviews import carga_trusted_reviews
from carga_reviews.carga_refined_reviews import carga_refined_reviews

# Importando os pipelines sellers
from carga_sellers.carga_raw_sellers import carga_raw_sellers
from carga_sellers.carga_trusted_sellers import carga_trusted_sellers
from carga_sellers.carga_refined_sellers import carga_refined_sellers


# importa a pasta funcao
current_dir = os.path.dirname(os.path.abspath(__file__))
funcoes_dir = os.path.join(current_dir, '..', 'funcoes')
sys.path.append(funcoes_dir)

# Cria uma linha abaixo da saída para dividir os logs
from funcoes import linha
from funcoes import sub_linha

pipeline_options = {
    'project': 'olist-brasil-project',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://olist_brasil_project/temp',
    'temp_location': 'gs://olist_brasil_project/temp',
    'template_location': 'gs://olist_brasil_project/template/batch_job_BQ_olist_full',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
#pipeline = beam.Pipeline(options=pipeline_options)
pipeline = beam.Pipeline()

serviceAccount = r'/home/michel/Documentos/Chave/olist-brasil-project-ee018150c520.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount


# Executar a função cargaKaggle()
def executa_carga_kaggle():
    try:
        cargaKaggle()
        print()
        print("|>>> Iniciando a carga dos arquivos...")
    except Exception as e:
        data_hora_atual = datetime.datetime.now()
        print(f'{data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f"Carga Kaggle -> Erro: {str(e)}")


#############################################################################################################################
##>> Carga Raw
# Função auxiliar para salvar a saída em um arquivo de log da carga RAW
def salvar_log_raw(nome_funcao, mensagem):
    arquivo_log = "/home/michel/Documentos/sistema_olist_GCP/Batch_Sistema_ETL_Olist/log/carga_raw.log"
    with open(arquivo_log, "a") as arquivo:
        data_hora_atual = datetime.datetime.now()
        linha_log = f"{'-' * 80} \n{data_hora_atual.strftime('%d/%m/%Y %H:%M:%S')} - {nome_funcao} - {mensagem}\n"
        arquivo.write(linha_log)


# Lista de funções a serem executadas na carga RAW
funcoes = [
    carga_raw_customers,
    carga_raw_geolocation,
    carga_raw_items,
    carga_raw_orders,
    carga_raw_payments,
    carga_raw_products,
    carga_raw_reviews,
    carga_raw_sellers
]

# Loop for para executar cada função da carga RAW
for funcao in funcoes:
    try:
        output = funcao()
        salvar_log_raw(funcao.__name__, "Sucesso")
        
    except Exception as e:
        data_hora_atual = datetime.datetime.now()
        sub_linha()
        mensagem_erro = f"{str(e)}"
        print(f'{data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f"{funcao.__name__} -> Erro: {mensagem_erro}")
        salvar_log_raw(funcao.__name__, mensagem_erro)
        linha()


#############################################################################################################################
##>> Carga Trusted
# Função auxiliar para salvar a saída em um arquivo de log da carga trusted
def salvar_log_trusted(nome_funcao, mensagem):
    arquivo_log = "/home/michel/Documentos/sistema_olist_GCP/Batch_Sistema_ETL_Olist/log/carga_trusted.log"
    with open(arquivo_log, "a") as arquivo:
        data_hora_atual = datetime.datetime.now()
        linha_log = f"{'-' * 80} \n{data_hora_atual.strftime('%d/%m/%Y %H:%M:%S')} - {nome_funcao} - {mensagem}\n"
        arquivo.write(linha_log)


# Lista de funções a serem executadas na carga trusted
funcoes = [
    carga_trusted_customers,
    carga_trusted_geolocation,
    carga_trusted_items,
    carga_trusted_orders,
    carga_trusted_payments,
    carga_trusted_products,
    carga_trusted_reviews,
    carga_trusted_sellers
]

# Loop for para executar cada função da carga trusted
for funcao in funcoes:
    try:
        output = funcao()
        salvar_log_trusted(funcao.__name__, "Sucesso")
        
    except Exception as e:
        data_hora_atual = datetime.datetime.now()
        sub_linha()
        mensagem_erro = f"{str(e)}"
        print(f'{data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f"{funcao.__name__} -> Erro: {mensagem_erro}")
        salvar_log_trusted(funcao.__name__, mensagem_erro)
        linha()

##############################################################################################################################
##>> Carga Refined
# Função auxiliar para salvar a saída em um arquivo de log da carga refined
def salvar_log_refined(nome_funcao, mensagem):
    arquivo_log = "/home/michel/Documentos/sistema_olist_GCP/Batch_Sistema_ETL_Olist/log/carga_refined.log"
    with open(arquivo_log, "a") as arquivo:
        data_hora_atual = datetime.datetime.now()
        linha_log = f"{'-' * 80} \n{data_hora_atual.strftime('%d/%m/%Y %H:%M:%S')} - {nome_funcao} - {mensagem}\n"
        arquivo.write(linha_log)


# Lista de funções a serem executadas na carga refined
funcoes = [
    carga_refined_customers,
    carga_refined_geolocation,
    carga_refined_items,
    carga_refined_orders,
    carga_refined_payments,
    carga_refined_products,
    carga_refined_reviews,
    carga_refined_sellers
]

# Loop for para executar cada função da carga refined
for funcao in funcoes:
    try:
        output = funcao()
        salvar_log_refined(funcao.__name__, "Sucesso")
        
    except Exception as e:
        data_hora_atual = datetime.datetime.now()
        sub_linha()
        mensagem_erro = f"{str(e)}"
        print(f'{data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")}')
        print(f"{funcao.__name__} -> Erro: {mensagem_erro}")
        salvar_log_refined(funcao.__name__, mensagem_erro)
        linha()

#
#
## Defina a função ou tarefa a ser executada
#def minha_tarefa():
#    executa_carga_kaggle()
#    executa()
#
## Executa a função imediatamente
#minha_tarefa()
#
## Agendado a execução da função a cada 20 minutos
#schedule.every(20).minutes.do(minha_tarefa)
#
#while True:
#    # Executa a tarefa agendada
#    schedule.run_pending()
#    time.sleep(1)
#
#