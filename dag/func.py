from dotenv import load_dotenv
import os
import sys

load_dotenv()
work_dir = os.getenv(r'E:/fincaraiz/')
sys.path.append(work_dir)



import requests
from bs4 import BeautifulSoup
from utils import get_dynamic_content_headless
import json
import time
import csv

import datetime


def web_scrapping(city, **kwargs):
    base_url = 'https://www.fincaraiz.com.co'

    url = f'https://www.fincaraiz.com.co/venta/casas/{city}'


    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'  # Para evitar bloqueos
    }

    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Buscar el contenedor principal
    search_container = soup.find('div', class_='search-page-container')

    # Asegúrate de que se encontró
    listings_wrapper = search_container.find('section', class_='listingsWrapper')
        
    # Buscar todas las tarjetas de propiedades
    tarjets = listings_wrapper.find_all('div', class_='listingCard highlight CO')

    now= datetime.datetime.now().strftime("%d/%m/%Y")

    with open(f'../data/{city}.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Escribe el encabezado
        writer.writerow(['Fecha', 'URL', 'Título', 'Constructor', 'Item', 'Unidades','Direccion_Principal', 'Direccion_Asociadas', 'lat/lon'])
        
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

            time.sleep(1)
            
            # Escribe la fila directamente en cada iteración
            writer.writerow([full_url, title, constructor, item, units, principal_address, associate_address,lat_lon, now])




