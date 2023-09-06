import apache_beam as beam
import csv
import datetime

# Objetos de organização do lyout do log/terminal
def registraDataLog():
    data_hora_atual = datetime.datetime.now()
    data_hora_atual.strftime("%d/%m/%Y %H:%M:%S")
    return data_hora_atual
    
def linha():
    print('_' * 80 + '\n')

def sub_linha():
    print('-' * 80)


# Funções de transformação
def extract_key_value(line):
    values = line.split(',')
    return (values[0], values[1:])
    
def expand_lists(element):
    key, value = element

    if isinstance(value['joins'], list):
        for sublist in value['joins']:
            yield (key, {'joins': sublist})
            

def convert_to_csv(element):
    key, value = element

    joined_values = [key] + value['joins']
    csv_row = ','.join(joined_values)

    return csv_row
'''
def parse_csv(line):
    columns = ['review_id'	'order_id','review_score','review_comment_title','review_comment_message','review_creation_date','review_answer_timestamp']  # Colunas do CSV
    values = line.split(',')  # Separar os campos por vírgula
    return dict(zip(columns, values))
'''

# Parse CSV customers
def parse_csv_customers(line):
    columns = ["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV geolocation
def parse_csv_geolocation(line):
    columns = ["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV items
def parse_csv_items(line):
    columns = ["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV payments
def parse_csv_payments(line):
    columns = ["order_id","payment_sequential","payment_type","payment_installments","payment_value"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV reviews
def parse_csv_reviews(line):
    columns = ["review_id","order_id","review_score","review_comment_title","review_comment_message","review_creation_date","review_answer_timestamp"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV orders
def parse_csv_orders(line):
    columns = ["order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV orders
def parse_csv_products(line):
    columns = ["product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))

# Parse CSV sellers
def parse_csv_sellers(line):
    columns = ["seller_id","seller_zip_code_prefix","seller_city","seller_state"]  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))


