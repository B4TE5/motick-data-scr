"""
Scraper Motick - Version Google Sheets OPTIMIZADA Y ANTI-DETECCION
Extrae datos de motos MOTICK y los sube directamente a Google Sheets

Version: 1.3 - CORREGIDO: Auto-update ChromeDriver + Anti-detección + Delays
CAMBIOS CLAVE:
- ChromeDriver se actualiza automáticamente
- Delays aleatorios entre anuncios (2-5 segundos)
- Rotación de User Agents
- Timeouts más robustos
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
    """Configura navegador Chrome con AUTO-UPDATE de ChromeDriver"""
    options = Options()
    
    # User Agent aleatorio para evitar detección
    ua = UserAgent()
    user_agent = ua.random
    options.add_argument(f"user-agent={user_agent}")
    
    # Configuraciones de velocidad + anti-detección
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
    
    # IMPORTANTE: Estas opciones ocultan que es un bot
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Suprimir logs
    options.add_argument("--log-level=3")
    
    # CLAVE: Usar ChromeDriverManager para auto-actualización
    try:
        service = Service(ChromeDriverManager().install())
        browser = webdriver.Chrome(service=service, options=options)
        print(f"[INFO] ChromeDriver actualizado correctamente")
        print(f"[INFO] User-Agent: {user_agent[:50]}...")
    except Exception as e:
        print(f"[ADVERTENCIA] Error con ChromeDriverManager: {e}")
        print(f"[INFO] Intentando con ChromeDriver del sistema...")
        browser = webdriver.Chrome(options=options)
    
    browser.implicitly_wait(0.5)  # Aumentado de 0.3 a 0.5
    
    # Ejecutar script para ocultar webdriver
    browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return browser

def safe_navigate(driver, url):
    """Navega con manejo de errores robusto"""
    try:
        driver.get(url)
        time.sleep(random.uniform(0.5, 1.0))  # Delay aleatorio
        return True
    except Exception:
        try:
            driver.get(url)
            time.sleep(random.uniform(0.8, 1.5))
            return True
        except:
            return False

def accept_cookies(driver):
    """Acepta cookies con timeout más largo"""
    try:
        cookie_button = WebDriverWait(driver, 3).until(  # Aumentado de 2 a 3
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookie_button.click()
        time.sleep(random.uniform(0.5, 1.0))  # Delay aleatorio
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
    
    # ESPERAR A QUE CARGUEN LOS PRECIOS (con timeout más largo)
    try:
        WebDriverWait(driver, 7).until(  # Aumentado de 5 a 7
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
        )
    except:
        pass
    
    # ESTRATEGIA 1: SELECTORES CSS ESPECÍFICOS DE WALLAPOP
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
    
    # ESTRATEGIA 2: XPATH POR ETIQUETA "Precio al contado"
    try:
        contado_elements = driver.find_elements(By.XPATH, 
            "//span[text()='Precio al contado']/following::span[contains(@class, 'ItemDetailPrice') and contains(text(), '€')]"
        )
        
        if contado_elements:
            raw_price = contado_elements[0].text.strip()
            return extract_price_from_text_wallapop(raw_price)
    except:
        pass
    
    # ESTRATEGIA 3: BUSCAR CUALQUIER PRECIO EN WALLAPOP
    try:
        price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '€')]")
        
        for element in price_elements:
            text = element.text.strip()
            if len(text) < 30 and '€' in text:
                price = extract_price_from_text_wallapop(text)
                if price != "No especificado":
                    try:
                        if 100 < int(price.replace(',', '')) < 1000000:
                            return price
                    except:
                        continue
    except:
        pass
    
    return "No especificado"

def extract_price_from_text_wallapop(text):
    """Extrae precio numerico de texto de Wallapop"""
    try:
        text = text.replace('\xa0', ' ').strip()
        match = re.search(r'([\d\s.,]+)\s*€', text)
        
        if match:
            price_str = match.group(1)
            price_str = price_str.replace(' ', '').replace('.', '')
            
            if ',' in price_str:
                parts = price_str.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    price_str = parts[0]
            
            price_int = int(price_str)
            
            if 100 < price_int < 1000000:
                return f"{price_int:,}".replace(',', '.')
            else:
                return "No especificado"
        
        return "No especificado"
    
    except Exception:
        return "No especificado"

def extract_km_robust(driver):
    """Extrae kilometraje usando MULTIPLES ESTRATEGIAS"""
    
    # ESTRATEGIA 1: Buscar elemento con icono de velocímetro
    try:
        km_elements = driver.find_elements(By.XPATH, 
            "//*[name()='svg' and contains(@class, 'icon-speedometer')]/ancestor::div[contains(@class, 'item-detail-characteristic')]//p"
        )
        
        for element in km_elements:
            text = element.text.strip()
            if 'km' in text.lower():
                km_value = extract_km_from_text(text)
                if km_value != "No especificado":
                    return km_value
    except:
        pass
    
    # ESTRATEGIA 2: Buscar por texto "km" o "Km" o "KM"
    try:
        all_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'km') or contains(text(), 'Km') or contains(text(), 'KM')]")
        
        for element in all_elements:
            text = element.text.strip()
            if len(text) < 50 and ('km' in text.lower()):
                km_value = extract_km_from_text(text)
                if km_value != "No especificado":
                    return km_value
    except:
        pass
    
    # ESTRATEGIA 3: Buscar en características del anuncio
    try:
        characteristics = driver.find_elements(By.CSS_SELECTOR, "[class*='characteristic']")
        
        for char in characteristics:
            text = char.text.strip()
            if 'km' in text.lower():
                km_value = extract_km_from_text(text)
                if km_value != "No especificado":
                    return km_value
    except:
        pass
    
    return "No especificado"

def extract_km_from_text(text):
    """Extrae valor numérico de kilometraje de texto"""
    try:
        text = text.lower().replace('\xa0', ' ').strip()
        
        patterns = [
            r'(\d+[\s\.]*\d*)\s*km',
            r'(\d+)\s*km',
            r'kilometraje[:\s]*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                km_str = match.group(1).replace(' ', '').replace('.', '')
                km_int = int(km_str)
                
                if 0 <= km_int <= 500000:
                    return f"{km_int:,}".replace(',', '.')
        
        return "No especificado"
        
    except Exception:
        return "No especificado"

def extract_year_robust(driver):
    """Extrae año del vehiculo"""
    
    # ESTRATEGIA 1: Buscar en características con icono calendario
    try:
        year_elements = driver.find_elements(By.XPATH, 
            "//*[name()='svg' and contains(@class, 'icon-calendar')]/ancestor::div[contains(@class, 'item-detail-characteristic')]//p"
        )
        
        for element in year_elements:
            text = element.text.strip()
            year = extract_year_from_text(text)
            if year != "No especificado":
                return year
    except:
        pass
    
    # ESTRATEGIA 2: Buscar años de 4 dígitos (1990-2025)
    try:
        all_text = driver.find_element(By.TAG_NAME, 'body').text
        years_found = re.findall(r'\b(19[9][0-9]|20[0-2][0-9])\b', all_text)
        
        if years_found:
            valid_years = [int(y) for y in years_found if 1990 <= int(y) <= 2025]
            if valid_years:
                return str(max(valid_years))  # El año más reciente encontrado
    except:
        pass
    
    return "No especificado"

def extract_year_from_text(text):
    """Extrae año de texto"""
    try:
        match = re.search(r'\b(19[9][0-9]|20[0-2][0-9])\b', text)
        if match:
            year = int(match.group(1))
            if 1990 <= year <= 2025:
                return str(year)
        return "No especificado"
    except:
        return "No especificado"

def extract_views_robust(driver):
    """Extrae número de visitas con MULTIPLES ESTRATEGIAS"""
    
    # ESTRATEGIA 1: Buscar por icono de ojo
    try:
        view_elements = driver.find_elements(By.XPATH, 
            "//*[name()='svg' and contains(@class, 'icon-eye')]/following-sibling::span"
        )
        
        for element in view_elements:
            text = element.text.strip()
            views = extract_number_from_text(text)
            if views is not None and views > 0:
                return views
    except:
        pass
    
    # ESTRATEGIA 2: Buscar por clase "views" o similar
    try:
        view_selectors = [
            "[class*='views']",
            "[class*='Views']",
            "[class*='visit']"
        ]
        
        for selector in view_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                views = extract_number_from_text(text)
                if views is not None and views > 0:
                    return views
    except:
        pass
    
    return 0

def extract_likes_robust(driver):
    """Extrae número de likes/favoritos"""
    
    # ESTRATEGIA 1: Buscar por icono de corazón
    try:
        like_elements = driver.find_elements(By.XPATH, 
            "//*[name()='svg' and contains(@class, 'icon-heart')]/following-sibling::span"
        )
        
        for element in like_elements:
            text = element.text.strip()
            likes = extract_number_from_text(text)
            if likes is not None and likes >= 0:
                return likes
    except:
        pass
    
    # ESTRATEGIA 2: Buscar por clase "favorite" o similar
    try:
        like_selectors = [
            "[class*='favorite']",
            "[class*='Favorite']",
            "[class*='like']",
            "[class*='heart']"
        ]
        
        for selector in like_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = element.text.strip()
                likes = extract_number_from_text(text)
                if likes is not None and likes >= 0:
                    return likes
    except:
        pass
    
    return 0

def extract_number_from_text(text):
    """Extrae número de texto (maneja formato con puntos/comas)"""
    try:
        text = text.replace('.', '').replace(',', '').strip()
        match = re.search(r'\d+', text)
        if match:
            return int(match.group())
        return None
    except:
        return None

def scroll_to_load_all_ads(driver, max_clicks=15, target_ads=300):
    """Carga todos los anuncios con sistema SMART + delays aleatorios"""
    
    print(f"[SMART] Objetivo: {target_ads} anuncios, máximo {max_clicks} clics")
    
    try:
        time.sleep(random.uniform(1.5, 2.5))  # Delay inicial aleatorio
        
        # Contar anuncios iniciales
        initial_ads = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/']"))
        print(f"[SMART] Anuncios iniciales: {initial_ads}")
        
        previous_count = initial_ads
        clicks_count = 0
        no_change_count = 0
        
        while clicks_count < max_clicks:
            try:
                # Buscar botón "Ver más"
                button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Ver más')]"))
                )
                
                # Scroll hasta el botón con comportamiento humano
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
                time.sleep(random.uniform(0.8, 1.5))  # Delay aleatorio antes de clic
                
                button.click()
                clicks_count += 1
                
                # Esperar carga con delay aleatorio
                time.sleep(random.uniform(2.0, 3.5))  # Delay aleatorio después de clic
                
                # Contar anuncios ahora
                current_ads = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/']"))
                new_ads = current_ads - previous_count
                
                print(f"[SMART] Clic {clicks_count}: {previous_count} → {current_ads} (+{new_ads})")
                
                # Si no se cargaron nuevos anuncios
                if new_ads == 0:
                    no_change_count += 1
                    if no_change_count >= 2:
                        print(f"[SMART] Sin nuevos anuncios tras {no_change_count} clics, finalizando")
                        break
                else:
                    no_change_count = 0
                
                # Si alcanzamos el objetivo
                if current_ads >= target_ads:
                    print(f"[SMART] Objetivo alcanzado: {current_ads} anuncios")
                    break
                
                previous_count = current_ads
                
            except TimeoutException:
                print(f"[SMART] Botón no encontrado, fin del contenido")
                break
            except Exception as e:
                print(f"[SMART] Error en clic: {str(e)}")
                break
        
        # Conteo final
        final_count = len(driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/']"))
        print(f"[SMART] Total final: {final_count} anuncios ({clicks_count} clics)")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Error cargando anuncios: {str(e)}")
        return False

def get_user_ads(driver, profile_url, account_name):
    """Extrae anuncios de perfil con DELAYS ALEATORIOS anti-detección"""
    
    all_ads = []
    
    try:
        print(f"[INFO] === PROCESANDO: {account_name} ===")
        print(f"[INFO] URL: {profile_url}")
        
        if not safe_navigate(driver, profile_url):
            print(f"[ERROR] No se pudo navegar a {profile_url}")
            return []
        
        # Aceptar cookies
        accept_cookies(driver)
        
        # Cargar todos los anuncios
        scroll_to_load_all_ads(driver)
        
        # Extraer enlaces
        ad_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/item/']")
        ad_urls = list(set([link.get_attribute('href') for link in ad_links if link.get_attribute('href')]))
        
        print(f"[INFO] Enlaces únicos: {len(ad_urls)}")
        
        if not ad_urls:
            print(f"[ADVERTENCIA] No se encontraron anuncios en {account_name}")
            return []
        
        # Procesar cada anuncio
        successful_ads = 0
        failed_ads = 0
        precios_ok = 0
        km_ok = 0
        
        # Barra de progreso
        pbar = tqdm(ad_urls, desc=f"Extrayendo {account_name}", 
                   bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
        
        for i, ad_url in enumerate(pbar, 1):
            try:
                # DELAY ALEATORIO ENTRE ANUNCIOS (2-5 segundos)
                delay = random.uniform(2.0, 5.0)
                time.sleep(delay)
                
                if not safe_navigate(driver, ad_url):
                    failed_ads += 1
                    continue
                
                # Esperar a que cargue la página
                time.sleep(random.uniform(1.0, 2.0))
                
                # Extraer datos
                titulo = extract_title_robust(driver)
                precio = extract_price_robust(driver)
                ano = extract_year_robust(driver)
                km = extract_km_robust(driver)
                visitas = extract_views_robust(driver)
                likes = extract_likes_robust(driver)
                
                # Contadores de calidad
                if precio != "No especificado":
                    precios_ok += 1
                if km != "No especificado":
                    km_ok += 1
                
                # Generar ID único
                id_base = f"{ad_url}_{account_name}_{titulo}_{km}"
                id_unico = hashlib.md5(id_base.encode()).hexdigest()[:12]
                
                ad_data = {
                    'ID_Moto': f"MOTICK-{id_unico}",
                    'Cuenta': account_name,
                    'Titulo': titulo,
                    'Precio': precio,
                    'Ano': ano,
                    'Kilometraje': km,
                    'Visitas': visitas,
                    'Likes': likes,
                    'URL': ad_url,
                    'Fecha_Extraccion': datetime.now().strftime("%d/%m/%Y"),
                    'ID_Unico_Real': id_base
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
    """Funcion principal del scraper MOTICK - VERSION CORREGIDA"""
    print("="*80)
    print("    MOTICK SCRAPER - VERSION CORREGIDA ANTI-DETECCION")
    print("="*80)
    print(" CARACTERISTICAS:")
    print("   • ChromeDriver se actualiza automáticamente")
    print("   • Delays aleatorios entre anuncios (2-5 seg)")
    print("   • Rotación de User Agents")
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
            print(f"[INFO] MODO TEST: Solo 2 cuentas")
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
                
                # Delay entre cuentas
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
            print(f"• Titulos: {titles_ok}/{total_processed} ({titles_ok/total_processed*100:.1f}%) ")
            print(f"• Precios: {prices_ok}/{total_processed} ({prices_ok/total_processed*100:.1f}%) ")
            print(f"• Kilometraje: {km_ok}/{total_processed} ({km_ok/total_processed*100:.1f}%) ")
            print(f"• Años: {years_ok}/{total_processed} ({years_ok/total_processed*100:.1f}%) ")
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
            fecha_extraccion = datetime.now().strftime("%d/%m/%y")
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
