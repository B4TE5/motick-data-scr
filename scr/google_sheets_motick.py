"""
Google Sheets Handler para Motick Scraper - CORREGIDO
Basado en el codigo que SI FUNCIONA del uploader
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import os
import time
import hashlib
import re
from datetime import datetime

class GoogleSheetsMotick:
    def __init__(self, credentials_json_string=None, sheet_id=None, credentials_file=None):
        """
        Inicializar handler con credenciales - IGUAL AL QUE FUNCIONA
        """
        if credentials_json_string:
            # Para GitHub Actions - desde string JSON
            credentials_dict = json.loads(credentials_json_string)
            self.credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
        elif credentials_file and os.path.exists(credentials_file):
            # Para testing local - desde archivo
            self.credentials = Credentials.from_service_account_file(
                credentials_file,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
        else:
            raise Exception("Se necesitan credenciales validas (JSON string o archivo)")
        
        self.client = gspread.authorize(self.credentials)
        self.sheet_id = sheet_id
        
        print("CONEXION: Google Sheets establecida correctamente")
        
    def test_connection(self):
        """Probar conexion - COPIA EXACTA DEL QUE FUNCIONA"""
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            print(f"CONEXION: Exitosa al Sheet: {spreadsheet.title}")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            return True
        except Exception as e:
            print(f"ERROR CONEXION: {str(e)}")
            print(f"SHEET_ID PROBLEMATICO: {self.sheet_id}")
            print("VERIFICAR: Que el ID corresponda a un GOOGLE SHEET NATIVO, no a un Excel")
            return False
    
    def crear_id_unico_real(self, fila):
        """
        Crea ID unico basado en: URL + cuenta + titulo + precio + km
        """
        try:
            url = str(fila.get('URL', '')).strip()
            cuenta = str(fila.get('Cuenta', '')).strip()
            titulo = str(fila.get('Titulo', '')).strip()
            precio = str(fila.get('Precio', '')).strip()
            km = str(fila.get('Kilometraje', '')).strip()
            
            titulo_limpio = re.sub(r'[^\w\s]', '', titulo.lower())[:30]
            km_limpio = re.sub(r'[^\d]', '', km)
            precio_limpio = re.sub(r'[^\d]', '', precio)
            
            clave_unica = f"{url}_{cuenta}_{titulo_limpio}_{precio_limpio}_{km_limpio}"
            
            return hashlib.md5(clave_unica.encode()).hexdigest()[:12]
            
        except Exception as e:
            url_safe = str(fila.get('URL', str(time.time())))
            return hashlib.md5(f"{url_safe}_{time.time()}".encode()).hexdigest()[:12]
    
    def subir_datos_scraper(self, df_motos, fecha_extraccion=None):
        """
        Sube datos - BASADO EN EL CODIGO QUE FUNCIONA
        """
        try:
            if fecha_extraccion is None:
                fecha_extraccion = datetime.now().strftime("%d/%m/%Y")
            
            # Crear ID_Unico_Real para cada moto
            df_motos['ID_Unico_Real'] = df_motos.apply(self.crear_id_unico_real, axis=1)
            
            # Nombre de hoja basado en fecha - MISMO PATRON QUE EL QUE FUNCIONA
            sheet_name = f"Datos_{fecha_extraccion.replace('/', '_')}"
            
            print(f"SUBIENDO: Datos a hoja {sheet_name}")
            
            # Abrir Google Sheet - IGUAL AL QUE FUNCIONA
            spreadsheet = self.client.open_by_key(self.sheet_id)
            print(f"ACCEDIENDO: Sheet {spreadsheet.title}")
            
            # Crear o limpiar worksheet - MISMO PATRON
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                worksheet.clear()
                print(f"LIMPIANDO: Hoja {sheet_name}")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df_motos) + 10,
                    cols=len(df_motos.columns) + 2
                )
                print(f"CREANDO: Nueva hoja {sheet_name}")
            
            # Preparar datos para subir - IDENTICO AL QUE FUNCIONA
            headers = df_motos.columns.values.tolist()
            data_rows = df_motos.values.tolist()
            all_data = [headers] + data_rows
            
            # Subir datos
            worksheet.update(all_data)
            
            print(f"SUBIDA EXITOSA: {sheet_name}")
            print(f"DATOS: {len(df_motos)} filas x {len(df_motos.columns)} columnas")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True, sheet_name
            
        except Exception as e:
            print(f"ERROR SUBIDA: {str(e)}")
            return False, None
    
    def leer_datos_historico(self, sheet_name="Data_Historico"):
        """
        Lee datos del historico
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                data = worksheet.get_all_values()
                
                if not data:
                    print(f"AVISO: Hoja {sheet_name} esta vacia")
                    return None
                
                headers = data[0]
                rows = data[1:]
                
                if not rows:
                    print(f"AVISO: Hoja {sheet_name} solo tiene headers")
                    return None
                
                df = pd.DataFrame(rows, columns=headers)
                
                # Limpiar columnas numericas
                columnas_visitas = [col for col in df.columns if col.startswith('Visitas_')]
                columnas_likes = [col for col in df.columns if col.startswith('Likes_')]
                
                for col in columnas_visitas + columnas_likes + ['Variacion_Likes']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
                print(f"LEIDO: {len(df)} motos del historico desde {sheet_name}")
                return df
                
            except gspread.WorksheetNotFound:
                print(f"AVISO: Hoja {sheet_name} no existe - sera creada en primera ejecucion")
                return None
                
        except Exception as e:
            print(f"ERROR LECTURA HISTORICO: {str(e)}")
            return None
    
    def leer_datos_scraper_reciente(self):
        """
        Lee los datos mas recientes del scraper
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            hojas_disponibles = [worksheet.title for worksheet in spreadsheet.worksheets()]
            
            print(f"DEBUG: Hojas disponibles: {hojas_disponibles}")
            
            # Buscar hojas de datos (formato: Datos_DD_MM_YYYY)
            hojas_datos = []
            for hoja in hojas_disponibles:
                if hoja.startswith('Datos_') and len(hoja.split('_')) == 4:
                    try:
                        fecha_str = hoja.replace('Datos_', '').replace('_', '/')
                        fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
                        hojas_datos.append((hoja, fecha_obj, fecha_str))
                    except:
                        continue
            
            if not hojas_datos:
                print("ERROR: No se encontraron hojas de datos del scraper")
                return None, None
            
            # Ordenar por fecha (mas reciente primero)
            hojas_datos.sort(key=lambda x: x[1], reverse=True)
            hoja_mas_reciente, fecha_obj, fecha_str = hojas_datos[0]
            
            print(f"LEYENDO: Datos mas recientes desde {hoja_mas_reciente} ({fecha_str})")
            
            # Leer la hoja mas reciente
            worksheet = spreadsheet.worksheet(hoja_mas_reciente)
            data = worksheet.get_all_values()
            
            if not data:
                print(f"ERROR: Hoja {hoja_mas_reciente} esta vacia")
                return None, None
            
            headers = data[0]
            rows = data[1:]
            
            if not rows:
                print(f"ERROR: Hoja {hoja_mas_reciente} solo tiene headers")
                return None, None
            
            df = pd.DataFrame(rows, columns=headers)
            
            # Limpiar columnas numericas
            df['Visitas'] = pd.to_numeric(df['Visitas'], errors='coerce').fillna(0).astype(int)
            df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce').fillna(0).astype(int)
            
            print(f"LEIDO: {len(df)} motos desde {hoja_mas_reciente}")
            return df, fecha_str
            
        except Exception as e:
            print(f"ERROR LECTURA SCRAPER: {str(e)}")
            return None, None
    
    def guardar_historico_con_hojas_originales(self, df_historico, fecha_procesamiento):
        """
        Guarda el historico en Data_Historico - SIMPLIFICADO
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # HOJA PRINCIPAL: Data_Historico
            try:
                worksheet_main = spreadsheet.worksheet("Data_Historico")
                worksheet_main.clear()
                print(f"LIMPIANDO: Hoja principal Data_Historico")
            except gspread.WorksheetNotFound:
                worksheet_main = spreadsheet.add_worksheet(
                    title="Data_Historico",
                    rows=len(df_historico) + 50,
                    cols=len(df_historico.columns) + 20
                )
                print(f"CREANDO: Nueva hoja Data_Historico")
            
            # Subir datos principales - IGUAL AL QUE FUNCIONA
            headers = df_historico.columns.values.tolist()
            data_rows = df_historico.values.tolist()
            all_data = [headers] + data_rows
            worksheet_main.update(all_data)
            
            print(f"EXITO: Historico actualizado en Data_Historico")
            print(f"DATOS: {len(df_historico)} filas x {len(df_historico.columns)} columnas")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            print(f"ERROR GUARDANDO HISTORICO: {str(e)}")
            return False

def test_google_sheets_motick():
    """Funcion de prueba IGUAL A LA QUE FUNCIONA"""
    print("PROBANDO CONEXION A GOOGLE SHEETS MOTICK")
    print("=" * 50)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    sheet_id = os.getenv('GOOGLE_SHEET_ID_MOTICK')
    print(f"SHEET_ID desde .env: {sheet_id}")
    
    if not sheet_id:
        sheet_id = input("Introduce el Sheet ID de MOTICK manualmente: ")
    
    try:
        # Intentar cargar credenciales locales
        credentials_file = "../credentials/service-account.json"
        if not os.path.exists(credentials_file):
            print(f"ERROR: No se encontro el archivo de credenciales: {credentials_file}")
            return False
        
        # Crear handler
        gs_handler = GoogleSheetsMotick(
            credentials_file=credentials_file,
            sheet_id=sheet_id
        )
        
        # Probar conexion
        if gs_handler.test_connection():
            print("\nCONEXION EXITOSA A MOTICK SHEETS")
            
            # Crear datos de prueba IGUAL AL QUE FUNCIONA
            test_data = {
                "ID_Moto": ["test1", "test2", "test3"],
                "Cuenta": ["MOTICK.TEST", "MOTICK.TEST", "MOTICK.TEST"],
                "Titulo": ["Test Moto 1", "Test Moto 2", "Test Moto 3"],
                "Precio": ["5000 EUR", "6000 EUR", "7000 EUR"],
                "Fecha_Extraccion": [datetime.now().strftime("%d/%m/%Y")] * 3
            }
            
            df_test = pd.DataFrame(test_data)
            print(f"SUBIENDO: Datos de prueba...")
            
            if gs_handler.subir_datos_scraper(df_test):
                print("EXITO: Datos de prueba subidos correctamente")
                print(f"REVISAR: https://docs.google.com/spreadsheets/d/{sheet_id}")
                return True
            else:
                print("ERROR: Subiendo datos de prueba")
                return False
        else:
            return False
            
    except Exception as e:
        print(f"ERROR EN PRUEBA: {str(e)}")
        return False

if __name__ == "__main__":
    # Ejecutar prueba si se ejecuta directamente
    test_google_sheets_motick()
