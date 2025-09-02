"""
Scraper Motick - Version Google Sheets CORREGIDA
Extrae datos de motos MOTICK y los sube directamente a Google Sheets

Version: 1.1 - Automatizada para GitHub Actions con BOTÓN CORREGIDO
Basado en: SCR_DATA_MOTICK.py original
CORRECCIÓN: Selectores mejorados para botón "Ver más productos"
"""

import time
import re
import os
import sys
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from tqdm import tqdm
import hashlib

# Importar modulos locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import get_motick_accounts, GOOGLE_SHEET_ID_MOTICK
from google_sheets_motick import GoogleSheetsMotick

def setup_browser():
    """Configura navegador Chrome ULTRA RAPIDO"""
    options = Options()
    
    # Configuraciones de maxima velocidad
    options.add_argument("--headless")  # IMPORTANTE: Headless para GitHub Actions
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-plugins")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-client-side-phishing-detection")
    options.add_argument("--disable-sync")
    options.add_argument("--disable-translate")
    options.add_argument("--hide-scrollbars")
    options.add_argument("--mute-audio")
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Suprimir logs completamente
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    browser = webdriver.Chrome(options=options)
    browser.implicitly_wait(0.5)  # Tiempo minimo
    return browser

def safe_navigate(driver, url):
    """Navega ULTRA RAPIDO sin reintentos innecesarios"""
    try:
        driver.get(url)
        time.sleep(0.3)  # Tiempo minimo
        return True
    except Exception:
        try:
            driver.get(url)
            time.sleep(0.5)
            return True
        except:
            return False

def accept_cookies(driver):
    """Acepta cookies de forma ultrarapida"""
    try:
        cookie_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_button.click()
        time.sleep(0.5)
        return True
    except:
        return False

def extract_title_robust(driver):
    """Extrae titulo con MULTIPLES ESTRATEGIAS ROBUSTAS"""
    
    # ESTRATEGIA 1: Selectores H1 genericos
    h1_selectors = [
        "h1",
        "h1[class*='title']",
        "h1[class*='Title']",
        "[class*='title'] h1",
        "[class*='Title'] h1"
    ]
    
    for selector in h1_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if text and len(text) > 3 and len(text) < 100:
                    # Validar que parece un titulo de moto
                    if any(word.upper() in text.upper() for word in ['HONDA', 'YAMAHA', 'KAWASAKI', 'SUZUKI', 'BMW', 'KTM', 'DUCATI', 'PIAGGIO', 'VESPA', 'APRILIA', 'TRIUMPH']):
                        return text
                    elif len(text) > 10:  # Si es suficientemente largo, probablemente es el titulo
                        return text
        except:
            continue
    
    # ESTRATEGIA 2: Buscar en metadatos
    try:
        title_meta = driver.find_element(By.XPATH, "//meta[@property='og:title']")
        content = title_meta.get_attribute("content")
        if content and len(content) > 5:
            return content.split(' - ')[0].strip()  # Quitar " - Wallapop"
    except:
        pass
    
    # ESTRATEGIA 3: Extraer desde la descripcion (primera linea)
    try:
        desc_selectors = [
            "[class*='description']",
            "section[class*='description']", 
            "div[class*='description']",
            "[class*='Description']"
        ]
        
        for selector in desc_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                desc_text = element.text.strip()
                if desc_text:
                    first_line = desc_text.split('\n')[0].strip()
                    if len(first_line) > 5 and len(first_line) < 80:
                        return first_line
            except:
                continue
    except:
        pass
    
    return "Titulo no encontrado"

def extract_price_robust(driver):
    """Extrae precio con MULTIPLES ESTRATEGIAS ROBUSTAS"""
    
    # ESTRATEGIA 1: Selectores de precio genericos
    price_selectors = [
        "span[class*='price']",
        "div[class*='price']",
        "span[class*='Price']",
        "div[class*='Price']",
        "[aria-label*='Price'] span",
        "[class*='amount']",
        "[class*='cost']"
    ]
    
    for selector in price_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if 'EUR' in text or 'euros' in text.lower():
                    price = extract_price_from_text(text)
                    if price != "No especificado":
                        return price
        except:
            continue
    
    # ESTRATEGIA 2: Buscar en metadatos
    try:
        price_meta = driver.find_element(By.XPATH, "//meta[@property='product:price:amount']")
        price_value = price_meta.get_attribute("content")
        if price_value:
            price_int = int(float(price_value))
            if 1000 <= price_int <= 50000:
                return f"{price_int:,} EUR".replace(',', '.')
    except:
        pass
    
    # ESTRATEGIA 3: Extraer de la descripcion completa
    try:
        desc_selectors = [
            "[class*='description']",
            "section", 
            "div[class*='content']"
        ]
        
        for selector in desc_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text
                    price = extract_price_from_text(text)
                    if price != "No especificado":
                        return price
            except:
                continue
    except:
        pass
    
    # ESTRATEGIA 4: Buscar en todo el texto de la pagina
    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text
        price = extract_price_from_text(page_text)
        if price != "No especificado":
            return price
    except:
        pass
    
    return "No especificado"

def extract_price_from_text(text):
    """Extrae precio de cualquier texto con REGEX AGRESIVOS"""
    if not text:
        return "No especificado"
    
    # Limpiar texto
    clean_text = text.replace('\u00a0', ' ').replace('&nbsp;', ' ')
    
    # PATRONES MULTIPLES para capturar diferentes formatos
    price_patterns = [
        r'precio[:\s]*(\d{1,2}\.?\d{3,6})\s*EUR',           # "Precio: 7.690 EUR"
        r'-\s*precio[:\s]*(\d{1,2}\.?\d{3,6})\s*EUR',       # "- Precio: 7.690 EUR"
        r'(\d{1,2}\.\d{3})\s*EUR',                          # "7.690 EUR"
        r'(\d{4,6})\s*EUR',                                 # "7690 EUR"
        r'(\d{1,2})\s*\.\s*(\d{3})\s*EUR',                  # "7 . 690 EUR"
        r'(\d{1,2}),(\d{3})\s*EUR',                         # "7,690 EUR"
        r'EUR\s*(\d{1,2}\.?\d{3,6})',                       # "EUR 7690"
        r'(\d{1,2}\.?\d{3,6})\s*euros?',                  # "7690 euros"
        r'vale\s+(\d{1,2}\.?\d{3,6})',                    # "vale 7690"
        r'cuesta\s+(\d{1,2}\.?\d{3,6})',                  # "cuesta 7690"
        r'por\s+(\d{1,2}\.?\d{3,6})\s*EUR',                 # "por 7690 EUR"
    ]
    
    for pattern in price_patterns:
        matches = re.finditer(pattern, clean_text, re.IGNORECASE)
        for match in matches:
            try:
                if len(match.groups()) == 2:  # Formato como 7.690
                    price_value = int(match.group(1) + match.group(2))
                else:
                    price_str = match.group(1).replace('.', '').replace(',', '')
                    price_value = int(price_str)
                
                # Validar rango realista para motos
                if 500 <= price_value <= 60000:
                    return f"{price_value:,} EUR".replace(',', '.')
            except:
                continue
    
    return "No especificado"

def extract_likes_robust(driver):
    """Extrae likes con MULTIPLES ESTRATEGIAS"""
    
    # ESTRATEGIA 1: Selectores especificos de favoritos
    like_selectors = [
        "button[aria-label*='favorite'] span",
        "button[aria-label*='Favorite'] span", 
        "[aria-label*='favorite']",
        "[aria-label*='Favorite']",
        "button[class*='favorite'] span",
        "button[class*='heart'] span",
        "[class*='favorite-counter']",
        "[class*='heart']"
    ]
    
    for selector in like_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                # Buscar numero en el texto
                text = element.text.strip()
                if text.isdigit() and 0 <= int(text) <= 1000:
                    return int(text)
                
                # Buscar en aria-label
                aria_label = element.get_attribute('aria-label') or ''
                numbers = re.findall(r'(\d+)', aria_label)
                if numbers:
                    likes_value = int(numbers[0])
                    if 0 <= likes_value <= 1000:
                        return likes_value
        except:
            continue
    
    # ESTRATEGIA 2: Buscar patron en todo el HTML
    try:
        page_source = driver.page_source
        like_patterns = [
            r'favorites.*?(\d+)',
            r'favorite.*?(\d+)',
            r'heart.*?(\d+)',
            r'(\d+).*?favorite',
            r'(\d+).*?heart'
        ]
        
        for pattern in like_patterns:
            matches = re.finditer(pattern, page_source, re.IGNORECASE)
            for match in matches:
                try:
                    likes_value = int(match.group(1))
                    if 0 <= likes_value <= 1000:
                        return likes_value
                except:
                    continue
    except:
        pass
    
    return 0

def extract_year_and_km_robust(driver):
    """Extrae año y KM con DETECCION MEJORADA para KM bajos"""
    year = "No especificado"
    km = "No especificado"
    
    # Obtener TODO el texto de la pagina
    try:
        # ESTRATEGIA MULTIPLE: Combinar descripcion + toda la pagina
        full_text = ""
        
        # 1. Descripcion especifica
        desc_selectors = [
            "[class*='description']",
            "section[class*='description']",
            "div[class*='content']",
            "section"
        ]
        
        for selector in desc_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements[:3]:  # Solo los primeros 3
                    full_text += " " + element.text
            except:
                continue
        
        # 2. Texto completo de la pagina
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text
            full_text += " " + page_text
        except:
            pass
        
        text_lower = full_text.lower()
        
        # EXTRAER AÑO con patrones multiples
        year_patterns = [
            r'año[:\s]*(\d{4})',                    # "Año: 2023"
            r'-\s*año[:\s]*(\d{4})',                # "- Año: 2023"
            r'año\s+(\d{4})',                       # "año 2023"
            r'modelo\s+(\d{4})',                    # "modelo 2023"
            r'del\s+año\s+(\d{4})',                 # "del año 2023"
            r'fabricacion[:\s]+(\d{4})',            # "fabricacion: 2023"
            r'(\d{4})\s*(?:cc|cilindros|EUR)',      # "2023 cc" contexto
            r'matriculacion[:\s]+(\d{4})'           # "matriculacion: 2023"
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, text_lower)
            if match:
                try:
                    year_value = int(match.group(1))
                    if 1990 <= year_value <= 2025:  # Rango mas amplio
                        year = str(year_value)
                        break
                except:
                    continue
        
        # EXTRAER KILOMETRAJE con DETECCION MEJORADA para KM bajos
        km_patterns = [
            r'kilometros[:\s]*(\d{1,3}(?:\.\d{3})+)',      # "Kilometros: 42.373"
            r'kilometros[:\s]*(\d{1,6})',                  # "Kilometros: 200" (KM BAJOS)
            r'-\s*kilometros[:\s]*(\d{1,6})',              # "- Kilometros: 10"
            r'km[:\s]*(\d{1,3}(?:\.\d{3})+)',              # "km: 42.373"
            r'km[:\s]*(\d{1,6})',                          # "km: 200" (KM BAJOS)
            r'(\d{1,3}\.\d{3})\s*km',                      # "42.373 km"
            r'(\d{1,6})\s*km(?!\w)',                       # "200 km" (sin otras letras despues)
            r'(\d+)\s*mil\s*km',                           # "42 mil km"
            r'recorridos[:\s]*(\d{1,6})',                  # "recorridos: 200"
            r'tiene[:\s]*(\d{1,6})\s*km',                  # "tiene 200 km"
            r'solo[:\s]*(\d{1,6})\s*km',                   # "solo 200 km"
            r'unicamente[:\s]*(\d{1,6})\s*km'              # "unicamente 10 km"
        ]
        
        for pattern in km_patterns:
            match = re.search(pattern, text_lower)
            if match:
                try:
                    km_text = match.group(1)
                    
                    # Manejar diferentes formatos
                    if '.' in km_text and len(km_text.split('.')[-1]) == 3:
                        # Formato 42.373
                        km_value = int(km_text.replace('.', ''))
                    elif 'mil' in pattern.lower():
                        # Formato "42 mil km"
                        km_value = int(km_text) * 1000
                    else:
                        # Formato directo
                        km_value = int(km_text)
                    
                    # VALIDACION MEJORADA para incluir KM muy bajos
                    if 0 < km_value <= 600000:  # Rango muy amplio
                        km = f"{km_value:,} km".replace(',', '.')
                        break
                        
                except:
                    continue
                    
    except Exception as e:
        pass
    
    return year, km

def extract_views_robust(driver):
    """Extrae visitas con multiples estrategias"""
    
    # ESTRATEGIA 1: Selectores especificos
    view_selectors = [
        'span[aria-label="Views"]',
        '[aria-label*="Views"]',
        '[aria-label*="views"]',
        '[class*="views"]',
        '[class*="Views"]'
    ]
    
    for selector in view_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if text.isdigit():
                    views = int(text)
                    if 0 <= views <= 100000:
                        return views
                        
                # Buscar en aria-label
                aria_label = element.get_attribute('aria-label') or ''
                numbers = re.findall(r'(\d+)', aria_label)
                if numbers:
                    views_value = int(numbers[0])
                    if 0 <= views_value <= 100000:
                        return views_value
        except:
            continue
    
    return 0

def create_moto_id(title, price, year, km):
    """Crea ID unico para detectar duplicados"""
    try:
        normalized_title = re.sub(r'[^\w\s]', '', title.lower().strip())[:20]
        normalized_price = re.sub(r'[^\d]', '', price)
        unique_string = f"{normalized_title}_{normalized_price}_{year}_{km}".replace(' ', '_')
        return hashlib.md5(unique_string.encode()).hexdigest()[:10]
    except:
        return hashlib.md5(str(time.time()).encode()).hexdigest()[:10]

def find_and_click_load_more(driver):
    """
    FUNCIÓN CORREGIDA: Busca y hace clic en 'Ver más productos' con selectores PRECISOS
    Basado en el HTML real: <walla-button text="Ver más productos" class="hydrated">
    """
    
    # SELECTORES CORREGIDOS basados en el HTML real
    selectors = [
        # Selector principal - walla-button con texto exacto
        ('css', 'walla-button[text="Ver más productos"]'),
        
        # Selector alternativo - walla-button con texto parcial
        ('css', 'walla-button[text*="Ver más"]'),
        
        # Button interno con clase específica
        ('css', 'button.walla-button__button'),
        ('css', '.walla-button__button'),
        
        # XPath para walla-button
        ('xpath', '//walla-button[@text="Ver más productos"]'),
        ('xpath', '//walla-button[contains(@text, "Ver más")]'),
        
        # XPath para span con texto dentro del botón
        ('xpath', '//span[text()="Ver más productos"]/ancestor::button'),
        ('xpath', '//span[contains(text(), "Ver más")]/ancestor::button'),
        ('xpath', '//span[text()="Ver más productos"]/ancestor::walla-button'),
        
        # Div container con justify-content-center
        ('css', '.d-flex.justify-content-center walla-button'),
        ('css', 'div[class*="justify-content-center"] walla-button'),
        
        # Fallback genéricos
        ('xpath', '//button[contains(@class, "walla-button")]'),
        ('xpath', '//*[contains(text(), "Ver más productos")]'),
        ('css', '[class*="load-more"]'),
        ('css', '[class*="more-items"]')
    ]
    
    print("[DEBUG] Buscando botón 'Ver más productos'...")
    
    for i, (selector_type, selector) in enumerate(selectors):
        try:
            print(f"[DEBUG] Probando selector {i+1}: {selector_type} = {selector}")
            
            if selector_type == 'css':
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            else:  # xpath
                elements = driver.find_elements(By.XPATH, selector)
            
            print(f"[DEBUG] Encontrados {len(elements)} elementos")
            
            for j, element in enumerate(elements):
                try:
                    # Verificar si el elemento está visible y habilitado
                    if not element.is_displayed():
                        print(f"[DEBUG] Elemento {j+1} no está visible")
                        continue
                        
                    if not element.is_enabled():
                        print(f"[DEBUG] Elemento {j+1} no está habilitado")
                        continue
                    
                    # Obtener texto para verificar
                    element_text = element.text.strip().lower()
                    print(f"[DEBUG] Elemento {j+1} texto: '{element_text}'")
                    
                    # Verificar que contiene "ver más" o es un botón de carga
                    if 'ver más' in element_text or 'ver mas' in element_text or not element_text:
                        print(f"[DEBUG] Intentando hacer clic en elemento {j+1}")
                        
                        # Scroll hacia el elemento
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        time.sleep(0.5)
                        
                        # Intentar clic normal
                        try:
                            element.click()
                            print(f"[SUCCESS] Clic exitoso con {selector_type}: {selector}")
                            time.sleep(1)
                            return True
                        except:
                            # Intentar clic con JavaScript
                            try:
                                driver.execute_script("arguments[0].click();", element)
                                print(f"[SUCCESS] Clic JS exitoso con {selector_type}: {selector}")
                                time.sleep(1)
                                return True
                            except Exception as e:
                                print(f"[DEBUG] Error en clic JS: {str(e)}")
                                continue
                    else:
                        print(f"[DEBUG] Elemento {j+1} no parece ser botón de 'Ver más'")
                        
                except Exception as e:
                    print(f"[DEBUG] Error procesando elemento {j+1}: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"[DEBUG] Error con selector {selector}: {str(e)}")
            continue
    
    print("[DEBUG] No se pudo encontrar/hacer clic en botón 'Ver más productos'")
    return False

def smart_load_all_ads(driver, expected_count=200, max_clicks=10):
    """
    FUNCIÓN MEJORADA: Carga todos los anuncios de forma inteligente y robusta
    """
    print(f"[SMART] Objetivo: {expected_count} anuncios, máximo {max_clicks} clics")
    
    # Scroll inicial para cargar contenido básico
    for i in range(3):
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(0.3)
    
    initial_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f"[SMART] Anuncios iniciales cargados: {initial_count}")
    
    clicks_realizados = 0
    last_count = initial_count
    
    for click_num in range(max_clicks):
        print(f"\n[SMART] Intento de clic {click_num + 1}/{max_clicks}")
        
        # Scroll hacia abajo para asegurar que el botón esté visible
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        # Intentar hacer clic en "Ver más productos"
        if find_and_click_load_more(driver):
            clicks_realizados += 1
            print(f"[SMART] Clic {clicks_realizados} realizado exitosamente")
            
            # Esperar a que se cargue nuevo contenido
            time.sleep(3)
            
            # Verificar si se cargó más contenido
            new_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
            print(f"[SMART] Anuncios después del clic: {new_count}")
            
            if new_count > last_count:
                print(f"[SMART] Se cargaron {new_count - last_count} anuncios nuevos")
                last_count = new_count
                
                # Si hemos alcanzado el objetivo, parar
                if new_count >= expected_count:
                    print(f"[SMART] Objetivo alcanzado: {new_count} >= {expected_count}")
                    break
            else:
                print(f"[SMART] No se cargaron anuncios nuevos, posible fin del contenido")
                break
        else:
            print(f"[SMART] No se pudo hacer clic, posible fin del contenido")
            break
    
    final_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f"\n[SMART] RESUMEN:")
    print(f"  • Anuncios iniciales: {initial_count}")
    print(f"  • Clics realizados: {clicks_realizados}")
    print(f"  • Anuncios finales: {final_count}")
    print(f"  • Anuncios nuevos cargados: {final_count - initial_count}")
    
    return final_count

def get_user_ads(driver, user_url, account_name):
    """Procesa todos los anuncios con extraccion ULTRA ROBUSTA"""
    print(f"\n[INFO] === PROCESANDO: {account_name} ===")
    print(f"[INFO] URL: {user_url}")
    
    all_ads = []
    
    try:
        if not safe_navigate(driver, user_url):
            print(f"[ERROR] No se pudo acceder al perfil")
            return all_ads
        
        accept_cookies(driver)
        
        # CARGA MEJORADA de anuncios con más clics
        final_count = smart_load_all_ads(driver, expected_count=300, max_clicks=15)
        
        ad_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]")
        ad_urls = list(set([elem.get_attribute('href') for elem in ad_elements if elem.get_attribute('href')]))
        
        print(f"[INFO] Enlaces únicos obtenidos: {len(ad_urls)}")
        
        successful_ads = 0
        failed_ads = 0
        
        for idx, ad_url in enumerate(tqdm(ad_urls, desc=f"Extrayendo {account_name}", colour="green")):
            try:
                if not safe_navigate(driver, ad_url):
                    failed_ads += 1
                    continue
                
                # EXTRACCION ROBUSTA con multiples estrategias
                title = extract_title_robust(driver)
                price = extract_price_robust(driver)
                likes = extract_likes_robust(driver)
                year, km = extract_year_and_km_robust(driver)
                views = extract_views_robust(driver)
                moto_id = create_moto_id(title, price, year, km)
                
                ad_data = {
                    'ID_Moto': moto_id,
                    'Cuenta': account_name,
                    'Titulo': title,
                    'Precio': price,
                    'Ano': year,
                    'Kilometraje': km,
                    'Visitas': views,
                    'Likes': likes,
                    'URL': ad_url,
                    'Fecha_Extraccion': datetime.now().strftime("%d/%m/%Y %H:%M")
                }
                
                all_ads.append(ad_data)
                successful_ads += 1
                
                # Delay ultra minimo
                time.sleep(0.1)
                
            except Exception as e:
                failed_ads += 1
                continue
    
    except Exception as e:
        print(f"[ERROR] Error procesando cuenta {account_name}: {str(e)}")
     
    print(f"[RESUMEN] {account_name}: {successful_ads} exitosos, {failed_ads} fallos")
    return all_ads

def main():
    """Funcion principal del scraper MOTICK automatizado"""
    print("="*80)
    print("    MOTICK SCRAPER - VERSION GOOGLE SHEETS AUTOMATIZADA")
    print("="*80)
    print(" CARACTERISTICAS:")
    print("   • Extraccion robusta con multiples estrategias")
    print("   • Subida directa a Google Sheets")
    print("   • Automatizado para GitHub Actions")
    print("   • Deteccion de KM bajos y datos precisos")
    print("   • CORREGIDO: Botón 'Ver más productos' funcional")
    print()
    
    try:
        # Configurar Google Sheets
        credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        sheet_id = os.getenv('GOOGLE_SHEET_ID') or GOOGLE_SHEET_ID_MOTICK
        
        if not credentials_json:
            print("[ERROR] Credenciales de Google no encontradas")
            return False
        
        if not sheet_id:
            print("[ERROR] ID de Google Sheet no encontrado")
            return False
        
        # Inicializar Google Sheets handler
        print("[INFO] Inicializando conexion a Google Sheets...")
        gs_handler = GoogleSheetsMotick(
            credentials_json_string=credentials_json,
            sheet_id=sheet_id
        )
        
        # Probar conexion
        if not gs_handler.test_connection():
            print("[ERROR] No se pudo conectar a Google Sheets")
            return False
        
        # Obtener cuentas MOTICK
        test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        motick_accounts = get_motick_accounts(test_mode)
        
        print(f"[INFO] Inicializando navegador...")
        driver = setup_browser()
        
        all_results = []
        total_expected = len(motick_accounts) * 250  # Estimacion mas realista
        
        print(f"[INFO] Procesando {len(motick_accounts)} cuentas MOTICK")
        
        start_time = time.time()
        
        for account_name, account_url in motick_accounts.items():
            print(f"\n{'='*60}")
            print(f"PROCESANDO: {account_name}")
            print(f"{'='*60}")
            
            try:
                account_ads = get_user_ads(driver, account_url, account_name)
                all_results.extend(account_ads)
                
                print(f"[RESUMEN] {account_name}: {len(account_ads)} anuncios procesados")
                
                time.sleep(2)  # Pausa entre cuentas
                
            except Exception as e:
                print(f"[ERROR] Error procesando {account_name}: {str(e)}")
                continue
        
        # Procesar y subir resultados
        if all_results:
            elapsed_time = (time.time() - start_time) / 60
            
            print(f"\n{'='*80}")
            print(f"PROCESAMIENTO COMPLETADO EN {elapsed_time:.1f} MINUTOS")
            print(f"{'='*80}")
            
            df = pd.DataFrame(all_results)
            df = df.sort_values(['Likes', 'Visitas'], ascending=[False, False])
            
            total_processed = len(df)
            total_likes = df['Likes'].sum()
            total_views = df['Visitas'].sum()
            
            # Calcular porcentajes de extraccion exitosa
            titles_ok = len(df[df['Titulo'] != 'Titulo no encontrado'])
            prices_ok = len(df[df['Precio'] != 'No especificado'])
            
            print(f"\nESTADISTICAS SCRAPER:")
            print(f"• Total anuncios procesados: {total_processed:,}")
            print(f"• Total visitas: {total_views:,}")
            print(f"• Total likes: {total_likes:,}")
            print(f"• Titulos extraidos: {titles_ok}/{total_processed} ({titles_ok/total_processed*100:.1f}%)")
            print(f"• Precios extraidos: {prices_ok}/{total_processed} ({prices_ok/total_processed*100:.1f}%)")
            print(f"• Media visitas: {df['Visitas'].mean():.1f}")
            print(f"• Media likes: {df['Likes'].mean():.1f}")
            print(f"• Tiempo total: {elapsed_time:.1f} minutos")
            
            # Subir a Google Sheets
            fecha_extraccion = datetime.now().strftime("%d/%m/%Y")
            print(f"\n[INFO] Subiendo datos a Google Sheets...")
            
            success, sheet_name = gs_handler.subir_datos_scraper(df, fecha_extraccion)
            
            if success:
                print(f"EXITO: Datos subidos correctamente a {sheet_name}")
                print(f"URL: https://docs.google.com/spreadsheets/d/{sheet_id}")
                return True
            else:
                print("[ERROR] Fallo al subir datos a Google Sheets")
                return False
            
        else:
            print("[ERROR] No se procesaron anuncios")
            return False
    
    except Exception as e:
        print(f"[ERROR] Error general: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        try:
            driver.quit()
        except:
            pass
        
        print(f"\nScraper MOTICK completado!")
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
