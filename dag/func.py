
import os
import sys

work_dir = "/home/jhona/housing-retail-pipeline"
if work_dir and work_dir not in sys.path:
    sys.path.append(work_dir)


import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.utils import get_dynamic_content_headless
from src.connection import get_engine
import json
import time
import csv

import datetime

def get_num(soup):
    pag_num = soup.select('ul.list li.ant-pagination-item')
    nums = []
    for li in pag_num:
        txt = li.get_text(strip=True)
        if txt.isdigit():
            nums.append(int(txt))
    return nums[-1]




def web_scrapping(city, **kwargs):
    base_url = 'https://www.fincaraiz.com.co'

    url = f'https://www.fincaraiz.com.co/venta/casas/{city}'


    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'  # Para evitar bloqueos
    }

    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')

    total = get_num(soup)


    now= datetime.datetime.now()
    city = city.split('/')

    department, city_dir = city[0], city[1]


    os.makedirs(f'./data/{department}', exist_ok=True)

    with open(f'./data/{department}/{city_dir}.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Escribe el encabezado
        writer.writerow(['Fecha', 'URL', 'Título', 'Constructor', 'Item', 'Unidades','full_info','Direccion_Principal', 'Direccion_Asociadas', 'lat/lon'])
        for i in range(1, total+1):
            url = f'https://www.fincaraiz.com.co/venta/casas/{department}/{city_dir}/pagina{i}'
            print(url)
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            # Buscar el contenedor principal
            search_container = soup.find('div', class_='search-page-container')

            # Asegúrate de que se encontró
            listings_wrapper = search_container.find('section', class_='listingsWrapper')
                
            # Buscar todas las tarjetas de propiedades
            tarjets = listings_wrapper.find_all(
                'div',
                class_=lambda x: x and 'listingCard' in x and 'CO' in x
            )
            for tarjet in tarjets:
                # Extracción de datos existente
                info = tarjet.find('div', class_='lc-dataWrapper')
                
                # Obtener URL
                a_tag = info.find('a', class_='lc-data')
                href = a_tag['href'] if a_tag and 'href' in a_tag.attrs else None
                full_url = base_url + href if href else "No disponible"
                
                # Obtener título
                title = a_tag['title']
                
                # Obtener constructor
                constructor = "No disponible"
                publisher = info.find('div', class_='publisher')
                if publisher:
                    strong_tag = publisher.find('strong')
                    if strong_tag:
                        constructor = strong_tag.text.strip()
                
                # Contenido dinámico
                content = get_dynamic_content_headless(full_url)

                principal_address = content[0]
                associate_address = content[1]
                item= content[2]
                units = content[3]
                lat_lon = content[4]
                full_info = content[5]

                time.sleep(1)
                
                # Escribe la fila directamente en cada iteración
                writer.writerow([now, full_url, title, constructor, item, units,full_info, principal_address, associate_address,lat_lon])
            


def save_in_db(city, **kwargs):
    city = city.split('/')

    department, city_dir = city[0], city[1]
    
    df = pd.read_csv(
        f'./data/{department}/{city_dir}.csv',
        quotechar='"',
        skipinitialspace=True)

    df.columns = df.columns.str.strip().str.replace("'", "")  

    table = 'address_data'
    engine = get_engine()

    df.rename(columns={
        "Fecha": "fecha",
        "URL": "url",
        "Título": "titulo",
        "Constructor": "constructor",
        "Item": "item",
        "Unidades": "unidades",
        "full_info": "full_info",
        "Direccion_Principal": "direccion_principal",
        "Direccion_Asociadas": "direccion_asociadas",
        "lat/lon": "lat_lon"
    }, inplace=True)


    df['department'] = department
    df['city'] = city_dir

    df.to_sql(
        name=table, 
        con=engine,              # <- SQLAlchemy Engine
        if_exists="append", 
        index=False
    )


def get_trigger_day(**kwargs):

    now= datetime.datetime.now().strftime("%d-%m-%Y")

    with open(f'./data/date/{now}.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Fecha'])
        writer.writerow([now])


