"""
Scraper Motick - Version Google Sheets OPTIMIZADA Y ANTI-DETECCION
Extrae datos de motos MOTICK y los sube directamente a Google Sheets

Version: 1.3 - CORREGIDO: Auto-update ChromeDriver + Anti-detección + Delays
CAMBIOS vs VERSION 1.2:
- ChromeDriver se actualiza automáticamente (CLAVE)
- User Agent aleatorio (anti-detección)
- Delays aleatorios entre anuncios 2-5 seg (anti-detección)
- MANTIENE: Toda la lógica de extracción y botones EXACTAMENTE igual
"""

import time
import re
import os
import sys
import pandas as pd
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from tqdm import tqdm
from fake_useragent import UserAgent
from webdriver_manager.chrome import ChromeDriverManager
import hashlib

# Importar modulos locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import get_motick_accounts, GOOGLE_SHEET_ID_MOTICK
from google_sheets_motick import GoogleSheetsMotick

def setup_browser():
    """Configura navegador Chrome con AUTO-UPDATE + anti-detección"""
    options = Options()
    
    # NUEVO: User Agent aleatorio
    try:
        ua = UserAgent()
        user_agent = ua.random
        options.add_argument(f"user-agent={user_agent}")
        print(f"[INFO] User-Agent aleatorio: {user_agent[:50]}...")
    except Exception as e:
        print(f"[ADVERTENCIA] No se pudo generar User-Agent aleatorio: {e}")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Configuraciones originales de velocidad
    options.add_argument("--headless")  
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
    
    # Anti-detección
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Suprimir logs
    options.add_argument("--log-level=3")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    # CLAVE: Auto-actualización de ChromeDriver
    try:
        service = Service(ChromeDriverManager().install())
        browser = webdriver.Chrome(service=service, options=options)
        print(f"[INFO] ChromeDriver actualizado correctamente")
    except Exception as e:
        print(f"[ADVERTENCIA] Error con ChromeDriverManager: {e}")
        print(f"[INFO] Intentando con ChromeDriver del sistema...")
        browser = webdriver.Chrome(options=options)
    
    browser.implicitly_wait(0.3)
    
    # Script anti-detección
    try:
        browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except:
        pass
    
    return browser

def safe_navigate(driver, url):
    """Navega ULTRA RAPIDO sin reintentos innecesarios"""
    try:
        driver.get(url)
        time.sleep(0.2)
        return True
    except Exception:
        try:
            driver.get(url)
            time.sleep(0.3)
            return True
        except:
            return False

def accept_cookies(driver):
    """Acepta cookies de forma ultrarapida"""
    try:
        cookie_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_button.click()
        time.sleep(0.3)
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
    """Extrae precio usando SELECTORES EXITOSOS del scraper de COCHES"""
    
    # ESPERAR A QUE CARGUEN LOS PRECIOS (copiado del scraper de coches)
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
        )
    except:
        pass
    
    # ESTRATEGIA 1: SELECTORES CSS ESPECÍFICOS DE WALLAPOP (del scraper de coches exitoso)
    price_selectors = [
        "span.item-detail-price_ItemDetailPrice--standardFinanced__f9ceG",
        ".item-detail-price_ItemDetailPrice--standardFinanced__f9ceG", 
        "span.item-detail-price_ItemDetailPrice--standard__fMa16",
        "span.item-detail-price_ItemDetailPrice--financed__LgMRH",
        ".item-detail-price_ItemDetailPrice--financed__LgMRH",
        "[class*='ItemDetailPrice']",
        "[class*='standardFinanced'] span",
        "[class*='financed'] span"
    ]
    
    for selector in price_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                if text and '€' in text:
                    price = extract_price_from_text_wallapop(text)
                    if price != "No especificado":
                        return price
        except:
            continue
    
    # ESTRATEGIA 2: XPATH POR ETIQUETA "Precio al contado" (del scraper de coches)
    try:
        contado_elements = driver.find_elements(By.XPATH, 
            "//span[text()='Precio al contado']/following::span[contains(@class, 'ItemDetailPrice') and contains(text(), '€')]"
        )
        
        if contado_elements:
            raw_price = contado_elements[0].text.strip()
            return extract_price_from_text_wallapop(raw_price)
    except:
        pass
    
    # ESTRATEGIA 3: BUSCAR CUALQUIER PRECIO EN WALLAPOP (método exitoso del scraper de coches)
    try:
        price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
        
        valid_prices = []
        for elem in price_elements[:10]:
            try:
                text = elem.text.strip().replace('&nbsp;', ' ').replace('\xa0', ' ')
                if not text:
                    continue
                
                # REGEX PARA CAPTURAR PRECIOS REALISTAS DE MOTOS
                price_patterns = [
                    r'(\d{1,3}(?:\.\d{3})+)\s*€',
                    r'(\d{1,6})\s*€'
                ]
                
                for pattern in price_patterns:
                    price_matches = re.findall(pattern, text)
                    for price_match in price_matches:
                        try:
                            price_clean = price_match.replace('.', '')
                            price_value = int(price_clean)
                            
                            # RANGO PARA MOTOS: 500€ - 60,000€
                            if 500 <= price_value <= 60000:
                                formatted_price = f"{price_value:,}".replace(',', '.') + " €" if price_value >= 1000 else f"{price_value} €"
                                valid_prices.append((price_value, formatted_price))
                        except:
                            continue
            except:
                continue
        
        # Tomar el precio más alto como precio principal
        if valid_prices:
            valid_prices = sorted(set(valid_prices), key=lambda x: x[0], reverse=True)
            return valid_prices[0][1]
                    
    except:
        pass
    
    return "No especificado"

def extract_price_from_text_wallapop(text):
    """Extrae precio de Wallapop - ADAPTADO del scraper de coches exitoso"""
    if not text:
        return "No especificado"
    
    # Limpiar texto (como en el scraper de coches exitoso)
    clean_text = text.replace('&nbsp;', ' ').replace('\xa0', ' ').strip()
    if not clean_text:
        return "No especificado"
    
    # REGEX ESPECÍFICOS PARA WALLAPOP (copiados del scraper de coches exitoso)
    price_patterns = [
        r'(\d{1,3}(?:\.\d{3})+)\s*€',           # "7.690 €"
        r'(\d{4,6})\s*€',                       # "7690 €"
        r'(\d{1,2})\s*\.\s*(\d{3})\s*€',        # "7 . 690 €"
        r'(\d{1,2}),(\d{3})\s*€',               # "7,690 €"
        r'€\s*(\d{1,2}\.?\d{3,6})',             # "€ 7690"
        r'(\d{1,2}\.?\d{3,6})\s*euros?',        # "7690 euros"
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
                
                # RANGO PARA MOTOS: 500€ - 60,000€
                if 500 <= price_value <= 60000:
                    return f"{price_value:,} €".replace(',', '.')
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
    """Extrae año y KM de la DESCRIPCIÓN de Wallapop - CORREGIDO PARA KM = 0"""
    year = "No especificado"
    km = "No especificado"
    
    try:
        # EXTRAER DE LA DESCRIPCIÓN usando selector específico de Wallapop
        description_selectors = [
            "section.item-detail_ItemDetailTwoColumns__description__0DKb0",
            ".item-detail_ItemDetailTwoColumns__description__0DKb0",
            "[class*='description']",
            "section[class*='description']"
        ]
        
        description_text = ""
        for selector in description_selectors:
            try:
                description_element = driver.find_element(By.CSS_SELECTOR, selector)
                description_text = description_element.text
                if description_text:
                    break
            except:
                continue
        
        if description_text:
            # EXTRAER KILÓMETROS de la descripción - PERMITIR KM = 0
            km_patterns = [
                r'-\s*Kilómetros:\s*(\d{1,3}(?:\.\d{3})*)',      # "- Kilómetros: 4.500"
                r'-\s*Kilómetros:\s*(\d+)',                      # "- Kilómetros: 0" PERMITIR 0
                r'-\s*kilómetros:\s*(\d{1,3}(?:\.\d{3})*)',      # "- kilómetros: 4.500"
                r'-\s*kilómetros:\s*(\d+)',                      # "- kilómetros: 0" PERMITIR 0  
                r'Kilómetros:\s*(\d{1,3}(?:\.\d{3})*)',          # "Kilómetros: 4.500"
                r'Kilómetros:\s*(\d+)',                          # "Kilómetros: 0" PERMITIR 0
                r'kilómetros:\s*(\d{1,3}(?:\.\d{3})*)',          # "kilómetros: 4.500"
                r'kilómetros:\s*(\d+)',                          # "kilómetros: 0" PERMITIR 0
                r'KM:\s*(\d{1,3}(?:\.\d{3})*)',                  # "KM: 4.500"
                r'KM:\s*(\d+)',                                  # "KM: 0" PERMITIR 0
                r'km:\s*(\d{1,3}(?:\.\d{3})*)',                  # "km: 4.500"
                r'km:\s*(\d+)',                                  # "km: 0" PERMITIR 0
                r'(\d{1,3}(?:\.\d{3})*)\s*km',                   # "4.500 km"
                r'(\d+)\s*km',                                   # "0 km" PERMITIR 0
                r'(\d{1,3}(?:\.\d{3})*)\s*kilómetros',           # "4.500 kilómetros"
                r'(\d+)\s*kilómetros',                           # "0 kilómetros" PERMITIR 0
                r'(\d+)\s*mil\s*km',                             # "42 mil km"
            ]
            
            for pattern in km_patterns:
                match = re.search(pattern, description_text, re.IGNORECASE)
                if match:
                    try:
                        km_text = match.group(1)
                        
                        # Manejar diferentes formatos
                        if 'mil' in pattern.lower():
                            # Formato "42 mil km"
                            km_value = int(km_text) * 1000
                        else:
                            # Formato normal con puntos como separadores de miles
                            km_value = int(km_text.replace('.', ''))
                        
                        # PERMITIR KM = 0 como valor válido
                        if 0 <= km_value <= 999999:  # CAMBIADO: ahora incluye 0
                            if km_value == 0:
                                km = "0 km"  # Formato específico para 0 km
                            else:
                                km = f"{km_value:,} km".replace(',', '.')
                            break
                            
                    except:
                        continue
            
            # EXTRAER AÑO de la descripción
            year_patterns = [
                r'-\s*Año:\s*(\d{4})',                          # "- Año: 2023"
                r'-\s*año:\s*(\d{4})',                          # "- año: 2023"
                r'Año:\s*(\d{4})',                              # "Año: 2023"
                r'año:\s*(\d{4})',                              # "año: 2023"
                r'modelo\s+(\d{4})',                            # "modelo 2023"
                r'del\s+(\d{4})',                               # "del 2023"
                r'(\d{4})\s*(?:cc|cilindros)',                  # "2023 cc"
            ]
            
            for pattern in year_patterns:
                match = re.search(pattern, description_text, re.IGNORECASE)
                if match:
                    try:
                        year_value = int(match.group(1))
                        if 1990 <= year_value <= 2025:
                            year = str(year_value)
                            break
                    except:
                        continue
        
        # Si no encuentra en descripción, buscar en HTML general (fallback)
        if km == "No especificado":
            try:
                html_content = driver.page_source
                km_patterns_html = [
                    r'Kilómetros["\s:>]*</span><span[^>]*>(\d+(?:[\.\s]\d+)*)</span>',
                    r'kilómetros["\s:>]*</span><span[^>]*>(\d+(?:[\.\s]\d+)*)</span>',
                    r'>(\d+)\s*km',  # PERMITIR 0 KM también en HTML
                ]
                
                for pattern in km_patterns_html:
                    matches = re.findall(pattern, html_content, re.IGNORECASE)
                    for match in matches:
                        try:
                            km_clean = match.replace('.', '').replace(',', '').replace(' ', '')
                            km_value = int(km_clean)
                            if 0 <= km_value <= 999999:  # INCLUIR 0
                                if km_value == 0:
                                    km = "0 km"
                                else:
                                    km = f"{km_value:,} km".replace(',', '.')
                                break
                        except:
                            continue
                    if km != "No especificado":
                        break
            except:
                pass
                    
    except Exception as e:
        pass
    
    return year, km

def extract_views_robust(driver):
    """Extrae visitas con multiples estrategias - CORREGIDO PARA FORMATO K"""
    
    # ESTRATEGIA 1: Selectores específicos
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
                
                # MANEJAR FORMATO K (1.1k = 1,100)
                if 'k' in text.lower():
                    try:
                        # Extraer número antes de 'k'
                        k_match = re.search(r'(\d+(?:\.\d+)?)\s*k', text.lower())
                        if k_match:
                            k_value = float(k_match.group(1))
                            views = int(k_value * 1000)
                            if 0 <= views <= 500000:  # Rango ampliado
                                return views
                    except:
                        pass
                
                # FORMATO NORMAL (número entero) - CAMBIADO elif por if
                if text.isdigit():
                    views = int(text)
                    if 0 <= views <= 500000:  # Rango ampliado
                        return views
                        
                # Buscar en aria-label
                aria_label = element.get_attribute('aria-label') or ''
                
                # MANEJAR FORMATO K EN ARIA-LABEL
                if 'k' in aria_label.lower():
                    try:
                        k_match = re.search(r'(\d+(?:\.\d+)?)\s*k', aria_label.lower())
                        if k_match:
                            k_value = float(k_match.group(1))
                            views = int(k_value * 1000)
                            if 0 <= views <= 500000:
                                return views
                    except:
                        continue
                
                # FORMATO NORMAL EN ARIA-LABEL
                numbers = re.findall(r'(\d+)', aria_label)
                if numbers:
                    views_value = int(numbers[0])
                    if 0 <= views_value <= 500000:
                        return views_value
        except:
            continue
    
    # ESTRATEGIA 2: Buscar en HTML completo formato K
    try:
        page_source = driver.page_source
        
        # Buscar patrones con K
        k_patterns = [
            r'(\d+(?:\.\d+)?)\s*k\s*views',
            r'views[^>]*>(\d+(?:\.\d+)?)\s*k',
            r'(\d+(?:\.\d+)?)\s*k\s*visitas'
        ]
        
        for pattern in k_patterns:
            matches = re.finditer(pattern, page_source, re.IGNORECASE)
            for match in matches:
                try:
                    k_value = float(match.group(1))
                    views = int(k_value * 1000)
                    if 0 <= views <= 500000:
                        return views
                except:
                    continue
        
        # Buscar patrones normales
        view_patterns = [
            r'views.*?(\d+)',
            r'view.*?(\d+)',
            r'(\d+).*?view'
        ]
        
        for pattern in view_patterns:
            matches = re.finditer(pattern, page_source, re.IGNORECASE)
            for match in matches:
                try:
                    views_value = int(match.group(1))
                    if 0 <= views_value <= 500000:
                        return views_value
                except:
                    continue
    except:
        pass
    
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
    Busca y hace clic en 'Ver más productos' - VERSION ORIGINAL SIN CAMBIOS
    """
    
    # SELECTORES ORIGINALES
    selectors = [
        ('css', 'walla-button[text="Ver más productos"]'),
        ('css', 'walla-button[text*="Ver más"]'),
        ('css', 'button.walla-button__button'),
        ('css', '.walla-button__button'),
        ('xpath', '//walla-button[@text="Ver más productos"]'),
        ('xpath', '//walla-button[contains(@text, "Ver más")]'),
        ('xpath', '//span[text()="Ver más productos"]/ancestor::button'),
        ('xpath', '//span[contains(text(), "Ver más")]/ancestor::button'),
        ('xpath', '//span[text()="Ver más productos"]/ancestor::walla-button'),
        ('css', '.d-flex.justify-content-center walla-button'),
        ('css', 'div[class*="justify-content-center"] walla-button'),
        ('xpath', '//button[contains(@class, "walla-button")]'),
        ('xpath', '//*[contains(text(), "Ver más productos")]'),
        ('css', '[class*="load-more"]'),
        ('css', '[class*="more-items"]')
    ]
    
    for selector_type, selector in selectors:
        try:
            if selector_type == 'css':
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
            else:  # xpath
                elements = driver.find_elements(By.XPATH, selector)
            
            for element in elements:
                try:
                    if not element.is_displayed() or not element.is_enabled():
                        continue
                    
                    # Verificar texto si es necesario
                    element_text = element.text.strip().lower()
                    if 'ver más' in element_text or 'ver mas' in element_text or not element_text:
                        # Scroll y clic
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        time.sleep(0.2)
                        
                        try:
                            element.click()
                            time.sleep(0.5)
                            return True
                        except:
                            try:
                                driver.execute_script("arguments[0].click();", element)
                                time.sleep(0.5)
                                return True
                            except:
                                continue
                except:
                    continue
        except:
            continue
    
    return False

def smart_load_all_ads(driver, expected_count=300, max_clicks=15):
    """
    Carga todos los anuncios de forma inteligente - VERSION ORIGINAL SIN CAMBIOS
    """
    print(f"[SMART] Objetivo: {expected_count} anuncios, máximo {max_clicks} clics")
    
    # Scroll inicial
    for i in range(2):
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(0.2)
    
    initial_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f"[SMART] Anuncios iniciales: {initial_count}")
    
    clicks_realizados = 0
    last_count = initial_count
    
    for click_num in range(max_clicks):
        # Scroll
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)
        
        if find_and_click_load_more(driver):
            clicks_realizados += 1
            
            # Espera para carga
            time.sleep(1.5)
            
            new_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
            
            if new_count > last_count:
                print(f"[SMART] Clic {clicks_realizados}: {last_count} → {new_count} (+{new_count - last_count})")
                last_count = new_count
                
                if new_count >= expected_count:
                    print(f"[SMART] Objetivo alcanzado")
                    break
            else:
                print(f"[SMART] Sin nuevos anuncios, fin del contenido")
                break
        else:
            print(f"[SMART] Botón no encontrado, fin del contenido")
            break
    
    final_count = len(driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]"))
    print(f"[SMART] Total final: {final_count} anuncios ({clicks_realizados} clics)")
    
    return final_count

def get_user_ads(driver, user_url, account_name):
    """Procesa todos los anuncios con extraccion ULTRA ROBUSTA - CON DELAYS ANTI-DETECCION"""
    print(f"\n[INFO] === PROCESANDO: {account_name} ===")
    print(f"[INFO] URL: {user_url}")
    
    all_ads = []
    
    try:
        if not safe_navigate(driver, user_url):
            print(f"[ERROR] No se pudo acceder al perfil")
            return all_ads
        
        accept_cookies(driver)
        
        # CARGA de anuncios (FUNCION ORIGINAL)
        final_count = smart_load_all_ads(driver, expected_count=300, max_clicks=15)
        
        ad_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/item/')]")
        ad_urls = list(set([elem.get_attribute('href') for elem in ad_elements if elem.get_attribute('href')]))
        
        print(f"[INFO] Enlaces únicos: {len(ad_urls)}")
        
        successful_ads = 0
        failed_ads = 0
        
        # CONTADORES PARA MONITORING RAPIDO
        precios_ok = 0
        km_ok = 0
        ejemplos_mostrados = 0
        
        for idx, ad_url in enumerate(tqdm(ad_urls, desc=f"Extrayendo {account_name}", colour="green")):
            try:
                # NUEVO: DELAY ALEATORIO ENTRE ANUNCIOS (2-5 segundos)
                delay = random.uniform(2.0, 5.0)
                time.sleep(delay)
                
                if not safe_navigate(driver, ad_url):
                    failed_ads += 1
                    continue
                
                # EXTRACCION ROBUSTA (ORIGINAL)
                title = extract_title_robust(driver)
                price = extract_price_robust(driver)
                likes = extract_likes_robust(driver)
                year, km = extract_year_and_km_robust(driver)
                views = extract_views_robust(driver)
                moto_id = create_moto_id(title, price, year, km)
                
                # CONTEO PARA MONITORING
                if price != "No especificado":
                    precios_ok += 1
                if km != "No especificado":
                    km_ok += 1
                
                # MOSTRAR EJEMPLOS DE LOS PRIMEROS 3 ANUNCIOS
                if ejemplos_mostrados < 3 and price != "No especificado" and km != "No especificado":
                    print(f"[EJEMPLO {ejemplos_mostrados + 1}] {title[:30]}... | {price} | {km} | {year}")
                    ejemplos_mostrados += 1
                
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
                
                # MOSTRAR PROGRESO CADA 50 ANUNCIOS
                if successful_ads % 50 == 0:
                    precio_pct = (precios_ok / successful_ads * 100) if successful_ads > 0 else 0
                    km_pct = (km_ok / successful_ads * 100) if successful_ads > 0 else 0
                    print(f"[PROGRESO] {successful_ads} procesados | Precios: {precio_pct:.1f}% | KM: {km_pct:.1f}%")
                
            except Exception as e:
                failed_ads += 1
                continue
    
    except Exception as e:
        print(f"[ERROR] Error procesando cuenta {account_name}: {str(e)}")
    
    # RESUMEN DETALLADO POR CUENTA
    if successful_ads > 0:
        precio_pct = (precios_ok / successful_ads * 100)
        km_pct = (km_ok / successful_ads * 100)
        print(f"[RESUMEN] {account_name}: {successful_ads} exitosos, {failed_ads} fallos")
        print(f"[CALIDAD] Precios: {precios_ok}/{successful_ads} ({precio_pct:.1f}%) | KM: {km_ok}/{successful_ads} ({km_pct:.1f}%)")
        
        # ALERTA SI CALIDAD BAJA
        if precio_pct < 70:
            print(f"[ALERTA] Baja extracción de precios en {account_name}")
        if km_pct < 60:
            print(f"[ALERTA] Baja extracción de KM en {account_name}")
    else:
        print(f"[RESUMEN] {account_name}: Sin anuncios procesados")
        
    return all_ads

def main():
    """Funcion principal del scraper MOTICK automatizado - VERSION CORREGIDA"""
    print("="*80)
    print("    MOTICK SCRAPER - VERSION CORREGIDA ANTI-DETECCION")
    print("="*80)
    print(" CARACTERISTICAS:")
    print("   • ChromeDriver se actualiza automáticamente")
    print("   • User Agent aleatorio (anti-detección)")
    print("   • Delays aleatorios entre anuncios (2-5 seg)")
    print("   • Extraccion robusta con multiples estrategias")
    print("   • Subida directa a Google Sheets")
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
        
        if test_mode:
            print(f"MODO TEST: Solo procesando 2 cuentas MOTICK")
        else:
            print(f"MODO COMPLETO: Procesando {len(motick_accounts)} cuentas MOTICK")
        
        print(f"[INFO] Inicializando navegador...")
        driver = setup_browser()
        
        all_results = []
        
        start_time = time.time()
        
        for account_name, account_url in motick_accounts.items():
            print(f"\n{'='*60}")
            print(f"PROCESANDO: {account_name}")
            print(f"{'='*60}")
            
            try:
                account_ads = get_user_ads(driver, account_url, account_name)
                all_results.extend(account_ads)
                
                print(f"[RESUMEN] {account_name}: {len(account_ads)} anuncios procesados")
                
                # NUEVO: Delay entre cuentas (3-7 segundos)
                time.sleep(random.uniform(3, 7))
                
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
            km_ok = len(df[df['Kilometraje'] != 'No especificado'])
            years_ok = len(df[df['Ano'] != 'No especificado'])
            
            print(f"\nESTADISTICAS SCRAPER:")
            print(f"• Total anuncios procesados: {total_processed:,}")
            print(f"• Total visitas: {total_views:,}")
            print(f"• Total likes: {total_likes:,}")
            print(f"• Tiempo total: {elapsed_time:.1f} minutos")
            print(f"\n CALIDAD DE EXTRACCIÓN:")
            print(f"• Titulos: {titles_ok}/{total_processed} ({titles_ok/total_processed*100:.1f}%) {'' if titles_ok/total_processed > 0.8 else '⚠️'}")
            print(f"• Precios: {prices_ok}/{total_processed} ({prices_ok/total_processed*100:.1f}%) {'' if prices_ok/total_processed > 0.7 else '⚠️'}")
            print(f"• Kilometraje: {km_ok}/{total_processed} ({km_ok/total_processed*100:.1f}%) {'' if km_ok/total_processed > 0.6 else '⚠️'}")
            print(f"• Años: {years_ok}/{total_processed} ({years_ok/total_processed*100:.1f}%) {'' if years_ok/total_processed > 0.5 else '⚠️'}")
            print(f"\n PROMEDIOS:")
            print(f"• Media visitas: {df['Visitas'].mean():.1f}")
            print(f"• Media likes: {df['Likes'].mean():.1f}")
            
            # MOSTRAR ALGUNOS EJEMPLOS FINALES
            print(f"\n EJEMPLOS DE DATOS EXTRAÍDOS:")
            samples = df.head(3)
            for i, (_, row) in enumerate(samples.iterrows(), 1):
                print(f"  {i}. {row['Titulo'][:40]}...")
                print(f"      {row['Precio']} |  {row['Kilometraje']} |  {row['Ano']} | 👁 {row['Visitas']} | ❤ {row['Likes']}")
            
            # ALERTAS DE CALIDAD
            alertas = []
            if titles_ok/total_processed < 0.8:
                alertas.append("Baja extracción de títulos")
            if prices_ok/total_processed < 0.7:
                alertas.append("Baja extracción de precios")
            if km_ok/total_processed < 0.6:
                alertas.append("Baja extracción de kilometraje")
                
            if alertas:
                print(f"\n⚠️  ALERTAS DE CALIDAD:")
                for alerta in alertas:
                    print(f"   • {alerta}")
            else:
                print(f"\n✅ CALIDAD EXCELENTE: Todos los indicadores están bien")
            
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
