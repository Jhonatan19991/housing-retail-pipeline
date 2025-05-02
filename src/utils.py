from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
import re

def get_location(driver):
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
    



def get_item(soup):


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



def get_units(soup):
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
                    }, ensure_ascii=False, indent=2) )
    return unidades



def get_dynamic_content_headless(url):
    opts = Options()

    opts.add_argument("--headless")

    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

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
    
    try:

        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "leaflet-control-container"))
        )
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CLASS_NAME, "units_items")))

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        # parseo
        lat, lon = get_location(driver)

        soup = BeautifulSoup(driver.page_source, 'html.parser')



        address = soup.select('div.location-header p')

        principal_address = address[1].get_text(strip=True)
        associate_address = address[3].get_text(strip=True)
        #get items
        items= get_item(soup)

        # get units
        units = get_units(soup)

        return [principal_address, associate_address, items, units, (lat, lon)]
    
    finally:
        driver.quit()
