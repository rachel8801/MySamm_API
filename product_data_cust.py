import pandas as pd
import http.client
import json
import time
from urllib.parse import urlencode
import pyodbc
from datetime import date
from datetime import datetime
from server_config import cnn_string

# Make an API call to get the export data info, such like total pages and current page
conn = http.client.HTTPSConnection("mschannellogin.com")
headers = {'content-type': "application/json"}
param_list = [{"website":"amazon"},
              {"website":"wayfair"},
              {"website":"walmart"},
              {"website":"overstock"},
              {"website":"homedepot"},
              {"website":"target"}]
# Iterate through the list of parameters to get each website total page and current page
def get_website_total_page():
    lst = []
    for params in param_list:
        encoded_params = urlencode(params)
        conn.request("GET", "/Api/cm_product_data_cust?api_key=123456789&"+ encoded_params, headers=headers)
        res = conn.getresponse()
        data = res.read()
        res_data = json.loads(data.decode("utf-8"))
        # print(res_data)
        dt = res_data['data']
        data_info=res_data['data_info']
        # print(data_info)
        total_page = data_info['total_page']
        current_page = data_info['current_page']
        arr = [params['website'], total_page, current_page]
        lst.append(arr)
    return lst

# Store the websites name, total_pages, and page into an array list
website_info = get_website_total_page()

# Create an Empty DataFrame object
df = pd.DataFrame()

# Counting the running time
t1= time.time()

# Open a connection to the server
conn = http.client.HTTPSConnection("mschannellogin.com")
# Iterate through the websites and pages
for website in website_info:
    website_name = website[0]
    pages = website[1]
    page_number = 1
    data_list = []

    # Iterate through the pages
    for i in range(pages):
        params = {"website": website_name, "page": page_number}
        encoded_params = urlencode(params)
        conn.request("GET", "/Api/cm_product_data_cust?api_key=123456789&" + encoded_params)
        res = conn.getresponse()
        data = res.read()
        res_data = json.loads(data.decode("utf-8"))
        data_list.append(res_data['data'])
        page_number += 1
    for data in data_list:
        df_1 = pd.DataFrame(data)
        df_1['Website'] = website_name
        df = pd.concat([df, df_1], axis=0)

# Finish the running and counting the total time
t2=time.time()
# print("Total time used:", t2-t1)
df.fillna("NULL", inplace=True)
df = df.drop_duplicates(subset=['product_sku', 'Website'], keep='last')

# Extract shipment arrival date from df['shipping_info']
df['arrival_date'] = df.shipping_info.str.split(':').str[-1].str.strip()
"""
Shipment arrival date has no year value, add the year info to the column
Logic: if arrival date after today's date, the year is this year; if arrival date before today's date,
then the year is next year
"""
today = date.today()
today_date = today.strftime('%m %d')
format_data = "%b %d"
shipment_arrival_date = []
for val in df['arrival_date'].tolist():
    try:
        date = datetime.strptime(val.strip(), format_data)
        arrival_dt = date.date().strftime('%m %d')

        if arrival_dt > today_date:
            shipment_arrival_date.append(arrival_dt + ' ' + str(today.year))
        else:
            shipment_arrival_date.append(arrival_dt + ' ' + str(today.year + 1))

    except ValueError as v:
        #print(v)
        shipment_arrival_date.append('')

df['shipment_arrival_date'] = shipment_arrival_date
df['shipment_arrival_date'] = df['shipment_arrival_date'].str.replace(' ', '/')
df.drop(['shipping_info','arrival_date'], axis=1, inplace=True)
columns_ls = df.columns.tolist()
columns_list = f'([{("],[".join(columns_ls))}])'
param_slots = '('+', '.join(['?']*len(df.columns))+')'

def update_data_cust_table():
    with pyodbc.connect(cnn_string) as conn:
        conn.timeout=3600
        curr = conn.cursor()
        sql = """TRUNCATE TABLE dbo.[product_data_cust]"""
        curr.execute(sql)
        #print("druncated")
        sql2 = "SET ANSI_WARNINGS off"
        curr.execute(sql2)
        ## Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            curr.execute(f""" INSERT INTO dbo.[product_data_cust]{columns_list} values{param_slots}""",
            row['product_sku'], row['website_sku'], row['Best_Seller_Rank_Category'], row['Best_Seller_Rank'],
            row['product_name'], row['price'], row['reviews'], row['rating'], row['images'], row['videos'],
            row['quick_ship'], row['product_url'], row['Website'],
            row['primary_category'], row['daily_rank'], row['shipment_arrival_date'])

        conn.commit()
    curr.close()
    conn.close()
