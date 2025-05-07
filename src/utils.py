"""
Utility functions for web scraping and data extraction from FincaRaiz.

This module contains functions for scraping real estate data from FincaRaiz website,
including property details, locations, and unit information.
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from bs4 import BeautifulSoup
import time
import json
import re
from typing import Tuple, List, Dict, Optional, Any

def get_location(driver: webdriver.Chrome) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract location coordinates from the page's network logs.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        Tuple[Optional[float], Optional[float]]: Latitude and longitude coordinates
    """
    logs = driver.get_log('performance')
    for entry in logs:
        log = json.loads(entry['message'])['message']
        if log['method'] == 'Network.responseReceived':
            url = log['params']['response']['url']
            if url.startswith('https://overpass-api.de/api/interpreter?data=[out:json];'):
                request_id = log['params']['requestId']
                response = driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': request_id})
                data = json.loads(response['body'])

                elements = data.get('elements', [])
                latitudes = []
                longitudes = []

                for element in elements:
                    if 'lat' in element and 'lon' in element:
                        latitudes.append(element['lat'])
                        longitudes.append(element['lon'])
                    elif element['type'] == 'way' and 'nodes' in element:
                        continue  # Los nodos de un way no tienen coordenadas aquí, necesitarías una consulta separada si los quieres

                if latitudes and longitudes:
                    bounding_box = {
                        'min_lat': min(latitudes),
                        'max_lat': max(latitudes),
                        'min_lon': min(longitudes),
                        'max_lon': max(longitudes)
                    }
                    lat = (bounding_box['max_lat'] + bounding_box['min_lat'])/2
                    lon = (bounding_box['max_lon'] + bounding_box['min_lon'])/2
                    return lat, lon
                        # otherwise fall back to parsing the bbox from the URL:
                # e.g. ...(11.0020,-74.8787,11.0479,-74.8239);...
                m = re.search(r'\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)', url)
                if m:
                    min_lat, min_lon, max_lat, max_lon = map(float, m.groups())
                    return ((min_lat + max_lat) / 2, (min_lon + max_lon) / 2)
    return None, None
    



def get_item(soup: BeautifulSoup) -> str:
    """
    Extract property details from the soup object.
    
    Args:
        soup: BeautifulSoup object of the property page
        
    Returns:
        str: JSON string containing property details
    """
    property_details = {}
    project_info = soup.find('ul', class_='ant-list-items')
    
    if project_info:
        list_items = project_info.find_all('li', class_='ant-list-item')
        for item in list_items:
            cols = item.find_all('div', class_='ant-col')
            if len(cols) == 2:
                key = cols[0].text.strip()
                value_text = cols[1].get_text(strip=True)
                property_details[key] = value_text if value_text not in ['¡Pregúntale!', ''] else "No disponible"

    json_item_data = json.dumps(property_details, ensure_ascii=False, indent=2)

    return json_item_data

def get_item_gold(soup: BeautifulSoup) -> str:
    """
    Extract detailed property information for premium listings.
    
    Args:
        soup: BeautifulSoup object of the property page
        
    Returns:
        str: JSON string containing detailed property information
    """
    property_details = {}
    # Buscamos el contenedor principal
    technical_sheet = soup.find('div', class_='jsx-952467510 technical-sheet')
    if not technical_sheet:
        return json.dumps(property_details, ensure_ascii=False, indent=2)

    # Cada fila es un div.ant-row.ant-row-space-between
    rows = technical_sheet.find_all('div', class_='ant-row ant-row-space-between')
    for row in rows:
        cols = row.find_all('div', class_='ant-col')
        if len(cols) < 3:
            continue

        # 1) Extraer clave: el segundo <span class="ant-typography"> en la primera columna
        label_col = cols[0]
        spans = label_col.find_all('span', class_='ant-typography')
        if len(spans) < 2:
            continue
        key = spans[1].get_text(strip=True)

        # 2) Extraer valor o marcar "No disponible" si hay botón
        value_col = cols[2]
        if value_col.find('button', class_='btnQuestion'):
            value = "No disponible"
        else:
            strong = value_col.find('strong')
            if strong:
                value = strong.get_text(strip=True)
            else:
                # como fallback, todo el texto de la columna
                value = value_col.get_text(strip=True) or "No disponible"

        property_details[key] = value
    
    container = soup.find('div', class_='property-price-tag')
    price_span = container.find('span', class_='ant-typography price heading heading-3 high')
    price = price_span.find('strong').text.strip()

    property_details['Precio'] = price
    
    return json.dumps(property_details, ensure_ascii=False, indent=2)

def get_units(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """
    Extract unit information from the property page.
    
    Args:
        soup: BeautifulSoup object of the property page
        
    Returns:
        List[Dict[str, str]]: List of dictionaries containing unit information
    """
    unidades = []
    units = soup.find_all("li", class_="proyect_units_list_item")
    
    for unidad in units:
        datos = unidad.find_all("div", class_="unit_item")
        info = {}
        for dato in datos:
            clave = dato.find_all("span")[0].get_text(strip=True).replace(":", "")
            valor = dato.find_all("span")[1].get_text(strip=True)
            info[clave] = valor
        unidades.append(info)
    
    if not unidades:
        table = soup.select_one('div.ant-table-container table')
        if table:
            for row in table.select('tbody tr'):
                cells = row.find_all('td')
                if len(cells) >= 6:
                    unidades.append(json.dumps({
                        'area_construida': cells[0].get_text(strip=True),
                        'area_privada':    cells[1].get_text(strip=True),
                        'tipo_inmueble':   cells[2].get_text(strip=True),
                        'hab_amb':         cells[3].get_text(strip=True),
                        'banos':           cells[4].get_text(strip=True),
                        'precio':          cells[5].get_text(strip=True),
                    }, ensure_ascii=False, indent=2))
    return unidades


def get_dynamic_content_headless(url: str) -> List[Any]:
    """
    Scrape dynamic content from FincaRaiz using headless Chrome.
    
    Args:
        url: URL of the property page to scrape
        
    Returns:
        List[Any]: List containing property information:
            - Principal address
            - Associated address
            - Property details
            - Unit information
            - Location coordinates
            - Premium listing details
    """
    opts = Options()

    opts.add_argument("--headless")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--remote-debugging-port=9222")  # Fuerza puerto DevTools  


    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    opts.add_experimental_option("prefs", prefs)
    opts.page_load_strategy = 'eager' 
    opts.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

    driver = webdriver.Chrome(
        options=opts
    )

    # Updated ChromeDriver initialization with error handling
    try:
        
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "leaflet-control-container"))
        )
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        lat, lon = get_location(driver)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        address = soup.select('div.location-header p')

        principal_address = address[1].get_text(strip=True)
        associate_address = address[3].get_text(strip=True)
        items = get_item(soup)
        units = get_units(soup)

        full_info = 0
        if not items or not units:
            full_info = get_item_gold(soup)
            
        return [principal_address, associate_address, items, units, (lat, lon), full_info]
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return ['','','','','','']
    finally:
        try:
            driver.quit()
        except:
            pass
