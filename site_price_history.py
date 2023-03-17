import pandas as pd
import http.client
import json
import pyodbc
from server_config import cnn_string

conn = http.client.HTTPSConnection("mschannellogin.com")
payload = ''
headers = {'content-type': "application/json"}

conn.request("GET", "/Api/produc_matrix_site_data?api_key=4mP32T61N769Bern1529", payload, headers=headers)
res = conn.getresponse()
data = res.read()
conn.close()
res_data = json.loads(data.decode("utf-8"))
#print(res_data)
dict_data = res_data['data']

df_list = []
# Loop through each product in the data
for product in dict_data.values():
    if isinstance(product, list):
        # Handle case where product is a list of dictionaries
        for prod in product:
            if 'sku' in prod:
                sku = prod['sku']
            else:
                sku = None
            if 'product_name' in prod:
                product_name = prod['product_name']
            else:
                product_name = None
            if 'map_price' in prod:
                map_price = prod['map_price']
            else:
                map_price = None
            if 'seller' in prod:
                seller_data = prod['seller']
                for seller in seller_data:
                    if 'seller_name' in seller:
                        seller_name = seller['seller_name']
                    else:
                        seller_name = None
                    website_url = seller['website_url']
                    for price_data in seller['price_data']:
                        for price in price_data:
                            date = price['date']
                            price = price['price']
                            # Create a new data frame with the current data
                            df = pd.DataFrame({
                                'sku': [sku],
                                'product_name': [product_name],
                                'map_price': [map_price],
                                'seller_name': [seller_name],
                                'website_url': [website_url],
                                'date': [date],
                                'price': [price]
                            })

                            # Append the new data frame to the list
                            df_list.append(df)
            else:
                seller_data = None

    elif isinstance(product, dict):
        # Handle case where product is a single dictionary
        if 'sku' in product:
            sku = product['sku']
        else:
            sku = None
        if 'product_name' in product:
            product_name = product['product_name']
        else:
            product_name = None
        if 'map_price' in product:
            map_price = product['map_price']
        else:
            map_price = None
        if 'seller' in product:
            seller_data = product['seller']
            for seller in seller_data:
                seller_name = seller['seller_name']
                website_url = seller['website_url']
                for price_data in seller['price_data']:
                    for price in price_data:
                        date = price['date']
                        price = price['price']

                        # Create a new data frame with the current data
                        df = pd.DataFrame({
                            'sku': [sku],
                            'product_name': [product_name],
                            'map_price': [map_price],
                            'seller_name': [seller_name],
                            'website_url': [website_url],
                            'date': [date],
                            'price': [price]
                        })

                        # Append the new data frame to the list
                        df_list.append(df)
# Concatenate all the data frames in the list
df = pd.concat(df_list, ignore_index=True)
df.drop_duplicates(keep='last', inplace=True)
#print(df.shape)
columns_ls = df.columns.tolist()
columns_list = f'([{("],[".join(columns_ls))}])'
param_slots = '('+', '.join(['?']*len(df.columns))+')'

def update_price_history_table():
    with pyodbc.connect(cnn_string) as conn:
        conn.timeout=3600
        curr = conn.cursor()
        sql = """TRUNCATE TABLE dbo.[site_price_history]"""
        curr.execute(sql)
        sql2 = "SET ANSI_WARNINGS off"
        curr.execute(sql2)
        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            curr.execute(f""" INSERT INTO dbo.[site_price_history]{columns_list} values{param_slots}""",
            row['sku'], row['product_name'], row['map_price'], row['seller_name'],
            row['website_url'], row['date'], row['price'])
        conn.commit()
    curr.close()
    conn.close()
