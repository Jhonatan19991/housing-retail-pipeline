from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json


def get_dynamic_content_headless(url):
    # 1) Preparo ChromeOptions
    opts = Options()
    # fuerza el nuevo headless en Chrome 109+:
    opts.add_argument("--headless")
    # en caso de que tu versión no lo soporte, prueba también:
    # opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    # opcional: no cargar imágenes/estilos/fuentes
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    opts.add_experimental_option("prefs", prefs)
    opts.page_load_strategy = 'eager'  # vuelve tras DOMContentLoaded

    # 2) Arranco Chrome *sin* ventana
    driver = webdriver.Chrome(
        options=opts
    )
    
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "ant-list-items"))
        )
        # parseo
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        property_details = {}
        project_info = soup.find('div', class_='project-info')
        
        if project_info:
            list_items = project_info.find_all('li', class_='ant-list-item')
            for item in list_items:
                cols = item.find_all('div', class_='ant-col')
                if len(cols) == 2:
                    key = cols[0].text.strip()
                    value_text = cols[1].get_text(strip=True)
                    property_details[key] = value_text if value_text not in ['¡Pregúntale!', ''] else "No disponible"

        json_item_data = json.dumps(property_details, ensure_ascii=False, indent=2)

        unidades = []
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
        return json_item_data, unidades
    
    finally:
        driver.quit()
