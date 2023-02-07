import pandas as pd
import numpy as np
import requests
import re
import os
from dotenv import load_dotenv
from os import path
import json



url = 'https://github.com/germanortola/data-pipelining-ironhack/blob/main/data/listings2.csv'

response = requests.get(url)
open("listings2.csv", "wb").write(response.content)

df = pd.read_csv('listings2.csv')

url2 = 'https://drive.google.com/file/d/19UzdboAjTDfAh9zNIKSWGkwByn1TX4so/view?usp=share_link'

response2 = requests.get(url2)
open("reviews2.csv", "wb").write(response.content)

df_reviews = pd.read_csv("reviews2.csv")

df2 = df.drop(columns=['listing_url', 'scrape_id', 'last_scraped', 'source', "picture_url", "host_thumbnail_url", "host_picture_url", "host_verifications", "calendar_updated", "calendar_last_scraped"], axis=1)

df_reviews['comments'] = df_reviews['comments'].astype(str)
df_reviews_grouped = df_reviews[['listing_id', 'date', 'comments']].groupby('listing_id').agg({'date': 'first', 'comments': ' '.join})
df3 = df2.merge(df_reviews_grouped, left_on='id', right_index=True)
df3['price_clean'] = [x.strip('$') for x in df3['price']]
df3["price_clean"]=df3["price_clean"].str.replace(',','')
df3["price_clean"].astype(float)
df3['price_clean'] = pd.to_numeric(df3['price_clean'], errors='coerce')
df3['price_clean'] = df3['price_clean'].round().astype(int)
df3 = df3[df3["price_clean"] < 20000]
df3_pivot_district = pd.pivot_table(df3, index='neighbourhood_group_cleansed', values=['id', 'beds_clean'], aggfunc={'id':'count', 'beds_clean':'sum'})
df3_pivot_district.rename(columns={'id':'total_airbnbs'}, inplace=True)
df3_pivot_district.sort_values(by='total_airbnbs', ascending=False, inplace=True)
df3_pivot_district.reset_index(inplace=True)
df3_pivot_district.rename(columns={'neighbourhood_group_cleansed':'district'}, inplace=True)
df3_pivot2 = df3.pivot_table(index='neighbourhood_cleansed', values=['id', 'beds_clean', 'price_clean'], aggfunc={'id':'count', 'beds_clean':'sum', 'price_clean':'mean'})
df3_pivot2.reset_index(inplace=True)
df3_pivot2.rename(columns={'neighbourhood_cleansed':'neighbourhood', 'price_clean':'price average', 'id':'total locations', 'beds_clean':'total beds'}, inplace=True)
df3_pivot2.sort_values(by='total locations', ascending=False, inplace=True)
df3_pivot2['price average'] = df3_pivot['price average'].astype(int)
max_idx_price = df3_pivot['price average'].idxmax()
max_ngbhd = df3_pivot.at[max_idx_price, 'neighbourhood']
max_avg_price = df3_pivot.at[max_idx_price, 'price average']
min_idx_price = df3_pivot['price average'].idxmin()
min_ngbhd = df3_pivot.at[min_idx_price, 'neighbourhood']
min_avg_price = df3_pivot.at[min_idx_price, 'price average']
df3.dropna(subset=['beds'], inplace=True)
df3['beds_clean'] = pd.to_numeric(df3['beds'], errors='coerce')
df3['beds_clean'] = df3['beds_clean'].round().astype(int)
df3_pivot_district = pd.pivot_table(df3, index='neighbourhood_group_cleansed', values=['id', 'beds_clean'], aggfunc={'id':'count', 'beds_clean':'sum'})
df3_pivot_district.rename(columns={'id':'total_airbnbs'}, inplace=True)
df3_pivot_district.sort_values(by='total_airbnbs', ascending=False, inplace=True)
df3_pivot_district.reset_index(inplace=True)
df3_pivot_district.rename(columns={'neighbourhood_group_cleansed':'district'}, inplace=True)
df3_distribution = df3.pivot_table(index='neighbourhood_group_cleansed', values='id', aggfunc='count')
df3_distribution.reset_index(inplace=True)
df3_distribution.rename(columns={'neighbourhood_group_cleansed':'district', 'id':'accommodations'}, inplace=True)
df3_distribution.sort_values(by='accommodations', ascending=True, inplace=True)
df3_distribution['accommodations'] = df3_distribution['accommodations'].astype(int)
price_mean = df3['price_clean'].mean()
price_median = df3['price_clean'].median()
price_mode = df3['price_clean'].mode().values[0]
price_max = df3['price_clean'].max()
price_min = df3['price_clean'].min()
price_std = df3['price_clean'].std()
df3_eda = pd.DataFrame({'price_mean': [price_mean], 'price_median': [price_median], 'price_mode': [price_mode], 'price_max': [price_max], 'price_min': [price_min], 'price_std': [price_std]})
url_viajeros_pernocte = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53822"
response_viajeros_pernocte = requests.request("GET", url_viajeros_pernocte)
viajeros_pernocte = response_viajeros_pernocte.json()
def INE_tables_qty(json):
    bcn_list = []
    num_list = []
    for i in json:
        for key1, val1 in i.items():
            if "Cataluña: Barcelona" in val1:
                bcn_list.append(val1)
        for key2, val2 in i.items():
            if "Data" in key2:
                for subdict in val2:
                    for key3, val3 in subdict.items():
                        if "Valor" in key3:
                            num_list.append(val3)

    return dict(zip(bcn_list, num_list))
dic_apt_qty_INE = INE_tables_qty(cant_apartamentos)
url_cant_apartamentos = "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53803"
response_cant_apartamentos = requests.request("GET", url_cant_apartamentos)
cant_apartamentos = response_cant_apartamentos.json()
def INE_tables_ocup(json):
    bcn_list = []
    num_list = []

    for i in json:
        for key1, val1 in i.items():
            if "Cataluña: Barcelona" in val1:
                bcn_list.append(val1)
        for key2, val2 in i.items():
            if "Data" in key2:
                for subdict in val2:
                    for key3, val3 in subdict.items():
                        if "Valor" in key3:
                            num_list.append(val3)

    return dict(zip(bcn_list, num_list))
dic_ocup_apt_INE = INE_tables_ocup(ocupacion_apartamentos)
