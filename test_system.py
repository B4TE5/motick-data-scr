#!/usr/bin/env python3
"""
Test Script para Motick Data Scraper
Ejecuta tests del sistema completo antes de deployment
"""

import os
import sys
import time
import traceback
from datetime import datetime

# Agregar src al path ANTES de cualquier import
script_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(script_dir, 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

def print_test_header(test_name):
    """Imprime header de test"""
    print(f"\n{'='*60}")
    print(f"TEST: {test_name}")
    print('='*60)

def print_test_result(test_name, success, message=""):
    """Imprime resultado del test"""
    status = "PASS" if success else "FAIL"
    print(f"[{status}] {test_name}")
    if message:
        print(f"      {message}")

def test_imports():
    """Test 1: Verificar que todos los módulos se importen correctamente"""
    print_test_header("Importación de Módulos")
    
    tests = []
    
    # Test importación de config
    try:
        from config import get_motick_accounts, GOOGLE_SHEET_ID_MOTICK
        tests.append(("Config module", True, "Configuración cargada"))
    except Exception as e:
        tests.append(("Config module", False, f"Error: {str(e)}"))
    
    # Test importación de Google Sheets
    try:
        from google_sheets_motick import GoogleSheetsMotick
        tests.append(("Google Sheets module", True, "Handler disponible"))
    except Exception as e:
        tests.append(("Google Sheets module", False, f"Error: {str(e)}"))
    
    # Test importación de scraper
    try:
        from scraper_motick import setup_browser, extract_title_robust
        tests.append(("Scraper module", True, "Funciones principales disponibles"))
    except Exception as e:
        tests.append(("Scraper module", False, f"Error: {str(e)}"))
    
    # Test importación de análisis
    try:
        from analisis_motick import AnalizadorHistoricoMotick
        tests.append(("Análisis module", True, "Analizador disponible"))
    except Exception as e:
        tests.append(("Análisis module", False, f"Error: {str(e)}"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def test_dependencies():
    """Test 2: Verificar dependencias críticas"""
    print_test_header("Dependencias del Sistema")
    
    critical_deps = [
        'selenium',
        'pandas', 
        'gspread',
        'google.oauth2',
        'tqdm',
        'openpyxl',
        'hashlib',
        'datetime'
    ]
    
    tests = []
    for dep in critical_deps:
        try:
            __import__(dep)
            tests.append((f"Dependency: {dep}", True, "Disponible"))
        except ImportError as e:
            tests.append((f"Dependency: {dep}", False, f"No encontrado: {str(e)}"))
    
    # Test versión de Python
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        tests.append((f"Python {version.major}.{version.minor}", True, "Versión compatible"))
    else:
        tests.append((f"Python {version.major}.{version.minor}", False, "Versión no compatible (necesita 3.8+)"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def test_configuration():
    """Test 3: Verificar configuración"""
    print_test_header("Configuración del Sistema")
    
    tests = []
    
    # Test carga de cuentas MOTICK
    try:
        from config import get_motick_accounts
        accounts_test = get_motick_accounts(test_mode=True)
        accounts_full = get_motick_accounts(test_mode=False)
        
        if len(accounts_test) >= 2:
            tests.append(("Cuentas test", True, f"{len(accounts_test)} cuentas configuradas"))
        else:
            tests.append(("Cuentas test", False, "Insuficientes cuentas de test"))
            
        if len(accounts_full) >= 10:
            tests.append(("Cuentas completas", True, f"{len(accounts_full)} cuentas configuradas"))
        else:
            tests.append(("Cuentas completas", False, "Insuficientes cuentas completas"))
            
    except Exception as e:
        tests.append(("Configuración MOTICK", False, f"Error: {str(e)}"))
    
    # Test variables de entorno
    env_vars = ['GOOGLE_SHEET_ID_MOTICK']
    for var in env_vars:
        if os.getenv(var):
            tests.append((f"ENV: {var}", True, "Configurado"))
        else:
            tests.append((f"ENV: {var}", False, "No configurado"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def test_google_sheets_connection():
    """Test 4: Conexión a Google Sheets (si hay credenciales)"""
    print_test_header("Conexión Google Sheets")
    
    tests = []
    
    try:
        from google_sheets_motick import GoogleSheetsMotick
        
        # Verificar credenciales
        credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        credentials_file = '../credentials/service-account.json'
        sheet_id = os.getenv('GOOGLE_SHEET_ID_MOTICK')
        
        if not credentials_json and not os.path.exists(credentials_file):
            tests.append(("Credenciales", False, "No encontradas (normal para setup inicial)"))
            return False
        
        if not sheet_id:
            tests.append(("Sheet ID", False, "GOOGLE_SHEET_ID_MOTICK no configurado"))
            return False
        
        # Intentar conexión
        if credentials_json:
            gs_handler = GoogleSheetsMotick(
                credentials_json_string=credentials_json,
                sheet_id=sheet_id
            )
        else:
            gs_handler = GoogleSheetsMotick(
                credentials_file=credentials_file,
                sheet_id=sheet_id
            )
        
        # Test conexión
        if gs_handler.test_connection():
            tests.append(("Conexión Google Sheets", True, "Conectado correctamente"))
        else:
            tests.append(("Conexión Google Sheets", False, "Error de conexión"))
        
    except Exception as e:
        tests.append(("Google Sheets", False, f"Error: {str(e)}"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def test_browser_setup():
    """Test 5: Setup del navegador"""
    print_test_header("Configuración del Navegador")
    
    tests = []
    
    try:
        from scraper_motick import setup_browser
        
        # Intentar crear instancia del navegador
        driver = setup_browser()
        tests.append(("Browser setup", True, "Chrome WebDriver inicializado"))
        
        # Test navegación básica  
        driver.get("https://www.google.com")
        if "Google" in driver.title:
            tests.append(("Browser navigation", True, "Navegación funcional"))
        else:
            tests.append(("Browser navigation", False, "Error en navegación"))
        
        driver.quit()
        
    except Exception as e:
        tests.append(("Browser setup", False, f"Error: {str(e)}"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def test_data_processing():
    """Test 6: Procesamiento de datos básico"""
    print_test_header("Procesamiento de Datos")
    
    tests = []
    
    try:
        import pandas as pd
        from analisis_motick import AnalizadorHistoricoMotick
        
        # Test creación de DataFrame
        sample_data = {
            'ID_Moto': ['test123'],
            'Cuenta': ['MOTICK.TEST'],
            'Titulo': ['Test Moto Honda CBR'],
            'Precio': ['5.000 EUR'],
            'Ano': ['2020'],
            'Kilometraje': ['15.000 km'],
            'Visitas': [100],
            'Likes': [25],
            'URL': ['https://test.com/item/123'],
            'Fecha_Extraccion': [datetime.now().strftime("%d/%m/%Y %H:%M")]
        }
        
        df = pd.DataFrame(sample_data)
        tests.append(("DataFrame creation", True, "Datos de prueba creados"))
        
        # Test normalizacion de columnas
        analizador = AnalizadorHistoricoMotick()
        df_normalized = analizador.normalizar_nombres_columnas(df.copy())
        tests.append(("Column normalization", True, "Columnas normalizadas"))
        
        # Test creación de ID único
        df_normalized['ID_Unico_Real'] = df_normalized.apply(analizador.crear_id_unico_real, axis=1)
        if len(df_normalized['ID_Unico_Real'].iloc[0]) == 12:
            tests.append(("ID generation", True, "ID único generado correctamente"))
        else:
            tests.append(("ID generation", False, "Error en generación de ID"))
        
    except Exception as e:
        tests.append(("Data processing", False, f"Error: {str(e)}"))
    
    # Mostrar resultados
    all_passed = True
    for test_name, success, message in tests:
        print_test_result(test_name, success, message)
        if not success:
            all_passed = False
    
    return all_passed

def main():
    """Función principal de testing"""
    print("="*60)
    print("MOTICK DATA SCRAPER - SYSTEM TESTS")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Cargar variables de entorno si existe .env
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Variables de entorno cargadas desde .env")
    except:
        print("Sin archivo .env (usando variables del sistema)")
    
    # Ejecutar todos los tests
    tests = [
        ("Importación de Módulos", test_imports),
        ("Dependencias del Sistema", test_dependencies), 
        ("Configuración", test_configuration),
        ("Google Sheets", test_google_sheets_connection),
        ("Navegador", test_browser_setup),
        ("Procesamiento de Datos", test_data_processing)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            start_time = time.time()
            success = test_func()
            duration = time.time() - start_time
            results.append((test_name, success, duration))
        except Exception as e:
            print(f"\nERROR EJECUTANDO {test_name}:")
            traceback.print_exc()
            results.append((test_name, False, 0))
    
    # Mostrar resumen final
    print("\n" + "="*60)
    print("RESUMEN DE TESTS")
    print("="*60)
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success, _ in results if success)
    failed_tests = total_tests - passed_tests
    
    for test_name, success, duration in results:
        status = "PASS" if success else "FAIL"
        print(f"[{status}] {test_name:<25} ({duration:.2f}s)")
    
    print(f"\nRESULTADO FINAL:")
    print(f"Total: {total_tests} | Passed: {passed_tests} | Failed: {failed_tests}")
    
    if failed_tests == 0:
        print("\nTODOS LOS TESTS PASARON - Sistema listo para usar!")
        return 0
    else:
        print(f"\n{failed_tests} TESTS FALLARON - Revisa los errores arriba")
        print("\nAcciones recomendadas:")
        if any("Credenciales" in name or "Google Sheets" in name for name, success, _ in results if not success):
            print("- Configura las credenciales de Google Sheets")
            print("- Verifica GOOGLE_SHEET_ID_MOTICK en .env")
        if any("Browser" in name for name, success, _ in results if not success):
            print("- Instala Google Chrome")
            print("- Verifica que selenium funcione correctamente")
        if any("Dependencias" in name for name, success, _ in results if not success):
            print("- Ejecuta: pip install -r requirements.txt")
        
        return 1

if __name__ == "__main__":
    sys.exit(main())