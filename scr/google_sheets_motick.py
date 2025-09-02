"""
Google Sheets Handler CORREGIDO para Motick Scraper
Version mejorada con diagnóstico y manejo robusto de errores
"""

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
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
        Inicializar handler con DIAGNÓSTICO AUTOMÁTICO
        """
        if credentials_json_string:
            credentials_dict = json.loads(credentials_json_string)
            self.credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            self.service_email = credentials_dict.get('client_email', 'NO_ENCONTRADO')
        elif credentials_file and os.path.exists(credentials_file):
            self.credentials = Credentials.from_service_account_file(
                credentials_file,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            # Leer email del archivo
            with open(credentials_file, 'r') as f:
                cred_data = json.load(f)
                self.service_email = cred_data.get('client_email', 'NO_ENCONTRADO')
        else:
            raise Exception("Se necesitan credenciales válidas (JSON string o archivo)")
        
        self.client = gspread.authorize(self.credentials)
        self.drive_service = build('drive', 'v3', credentials=self.credentials)
        self.sheet_id = sheet_id
        
        print(f"CONEXIÓN: Google Sheets inicializada")
        print(f"SERVICE ACCOUNT: {self.service_email}")
        print(f"SHEET ID: {self.sheet_id}")
        
    def test_connection(self):
        """
        Prueba conexión con DIAGNÓSTICO DETALLADO
        """
        try:
            print(f"\n[DIAGNÓSTICO] Probando conexión...")
            
            # Test 1: Verificar que el documento existe con Drive API
            print(f"[TEST 1] Verificando documento en Drive...")
            try:
                file_info = self.drive_service.files().get(
                    fileId=self.sheet_id,
                    fields='id,name,mimeType,owners,shared'
                ).execute()
                
                print(f"✅ Documento encontrado: {file_info['name']}")
                print(f"📄 Tipo MIME: {file_info['mimeType']}")
                
                # Verificar que es Google Sheet nativo
                if file_info['mimeType'] != 'application/vnd.google-apps.spreadsheet':
                    print(f"❌ PROBLEMA: No es Google Sheet nativo")
                    print(f"   Tipo actual: {file_info['mimeType']}")
                    print(f"   Necesitas convertirlo a Google Sheet")
                    return False
                
            except Exception as e:
                print(f"❌ Error accediendo documento: {str(e)}")
                print(f"💡 Posibles causas:")
                print(f"   • Sheet ID incorrecto: {self.sheet_id}")
                print(f"   • Documento no existe")
                return False
            
            # Test 2: Verificar permisos con Sheets API
            print(f"[TEST 2] Probando acceso con Sheets API...")
            try:
                spreadsheet = self.client.open_by_key(self.sheet_id)
                print(f"✅ Conectado a: {spreadsheet.title}")
                
                # Listar hojas disponibles
                worksheets = spreadsheet.worksheets()
                worksheet_names = [ws.title for ws in worksheets]
                print(f"📊 Hojas encontradas: {worksheet_names}")
                
                return True
                
            except gspread.exceptions.APIError as api_error:
                error_details = str(api_error)
                print(f"❌ ERROR DE API: {error_details}")
                
                if "This operation is not supported" in error_details:
                    print(f"🔧 DIAGNÓSTICO: Documento no es Google Sheet nativo")
                    print(f"📋 SOLUCIÓN: Abre en Google Drive → Clic derecho → Abrir con Google Sheets → Guardar como Google Sheets")
                elif "does not have permission" in error_details or "PERMISSION_DENIED" in error_details:
                    print(f"🔧 DIAGNÓSTICO: Problemas de permisos")
                    print(f"📋 SOLUCIÓN CRÍTICA:")
                    print(f"   1. Abre el Google Sheet: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
                    print(f"   2. Clic en 'Compartir' (botón azul)")
                    print(f"   3. Agregar email: {self.service_email}")
                    print(f"   4. Dar permisos: Editor")
                    print(f"   5. Clic en 'Enviar'")
                elif "insufficientPermissions" in error_details:
                    print(f"🔧 DIAGNÓSTICO: Scopes insuficientes")
                    print(f"📋 VERIFICAR: APIs habilitadas en Google Cloud Console")
                else:
                    print(f"🔧 DIAGNÓSTICO: Error API desconocido")
                
                return False
                
            except Exception as e:
                print(f"❌ ERROR GENERAL: {str(e)}")
                return False
        
        except Exception as e:
            print(f"❌ ERROR CRÍTICO: {str(e)}")
            return False
    
    def crear_id_unico_real(self, fila):
        """
        Crea ID único basado en: URL + cuenta + título + precio + km
        """
        try:
            url = str(fila.get('URL', '')).strip()
            cuenta = str(fila.get('Cuenta', '')).strip()
            titulo = str(fila.get('Titulo', '')).strip()
            precio = str(fila.get('Precio', '')).strip()
            km = str(fila.get('Kilometraje', '')).strip()
            
            # Normalizar título (quitar caracteres especiales)
            titulo_limpio = re.sub(r'[^\w\s]', '', titulo.lower())[:30]
            km_limpio = re.sub(r'[^\d]', '', km)
            precio_limpio = re.sub(r'[^\d]', '', precio)
            
            # Crear clave única
            clave_unica = f"{url}_{cuenta}_{titulo_limpio}_{precio_limpio}_{km_limpio}"
            
            # Hash para ID manejable
            return hashlib.md5(clave_unica.encode()).hexdigest()[:12]
            
        except Exception as e:
            # Fallback con URL + timestamp
            url_safe = str(fila.get('URL', str(time.time())))
            return hashlib.md5(f"{url_safe}_{time.time()}".encode()).hexdigest()[:12]
    
    def subir_datos_scraper(self, df_motos, fecha_extraccion=None):
        """
        Sube datos del scraper con REINTENTOS y MEJOR MANEJO DE ERRORES
        """
        max_reintentos = 3
        
        for intento in range(max_reintentos):
            try:
                if fecha_extraccion is None:
                    fecha_extraccion = datetime.now().strftime("%d/%m/%Y")
                
                # Crear ID_Unico_Real para cada moto
                df_motos['ID_Unico_Real'] = df_motos.apply(self.crear_id_unico_real, axis=1)
                
                # Nombre de hoja basado en fecha
                sheet_name = f"Datos_{fecha_extraccion.replace('/', '_')}"
                
                print(f"\n[INTENTO {intento + 1}] Subiendo datos a hoja {sheet_name}")
                
                spreadsheet = self.client.open_by_key(self.sheet_id)
                
                # Verificar si ya existe una hoja con este nombre
                try:
                    existing_sheet = spreadsheet.worksheet(sheet_name)
                    print(f"AVISO: Hoja {sheet_name} existe - limpiando datos")
                    existing_sheet.clear()
                    worksheet = existing_sheet
                except gspread.WorksheetNotFound:
                    # Crear nueva hoja
                    worksheet = spreadsheet.add_worksheet(
                        title=sheet_name,
                        rows=len(df_motos) + 20,
                        cols=len(df_motos.columns) + 5
                    )
                    print(f"CREANDO: Nueva hoja {sheet_name}")
                
                # Preparar datos para subir
                headers = df_motos.columns.values.tolist()
                data_rows = df_motos.values.tolist()
                all_data = [headers] + data_rows
                
                # Subir datos con batch update (más eficiente)
                worksheet.update(all_data)
                
                print(f"EXITO: {len(df_motos)} motos subidas a {sheet_name}")
                print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
                
                return True, sheet_name
                
            except gspread.exceptions.APIError as api_error:
                print(f"ERROR API (intento {intento + 1}): {str(api_error)}")
                
                if "This operation is not supported" in str(api_error):
                    print(f"❌ CRÍTICO: Google Sheet no es nativo - necesita conversión")
                    return False, None
                elif "does not have permission" in str(api_error):
                    print(f"❌ CRÍTICO: Sin permisos - compartir sheet con {self.service_email}")
                    return False, None
                elif intento == max_reintentos - 1:
                    print(f"❌ FALLO FINAL después de {max_reintentos} intentos")
                    return False, None
                else:
                    print(f"⏳ Esperando antes del siguiente intento...")
                    time.sleep(2 ** intento)  # Backoff exponencial
                    
            except Exception as e:
                print(f"ERROR GENERAL (intento {intento + 1}): {str(e)}")
                if intento == max_reintentos - 1:
                    return False, None
                time.sleep(2)
        
        return False, None
    
    def leer_datos_historico(self, sheet_name="Data_Historico"):
        """
        Lee datos del histórico con MEJOR MANEJO DE ERRORES
        """
        try:
            print(f"[LECTURA] Accediendo a hoja {sheet_name}...")
            
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                
                # Obtener todos los valores
                all_values = worksheet.get_all_values()
                
                if not all_values:
                    print(f"AVISO: Hoja {sheet_name} está vacía")
                    return None
                
                # Convertir a DataFrame
                headers = all_values[0]
                rows = all_values[1:]
                
                if not rows:
                    print(f"AVISO: Hoja {sheet_name} solo tiene headers")
                    return None
                
                df = pd.DataFrame(rows, columns=headers)
                
                # Limpiar columnas numéricas
                columnas_visitas = [col for col in df.columns if col.startswith('Visitas_')]
                columnas_likes = [col for col in df.columns if col.startswith('Likes_')]
                
                for col in columnas_visitas + columnas_likes + ['Variacion_Likes']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                
                print(f"LEÍDO: {len(df)} motos del histórico desde {sheet_name}")
                print(f"COLUMNAS: {len(df.columns)} total")
                return df
                
            except gspread.WorksheetNotFound:
                print(f"AVISO: Hoja {sheet_name} no existe - será creada en primera ejecución")
                return None
                
        except Exception as e:
            print(f"ERROR LECTURA HISTÓRICO: {str(e)}")
            return None
    
    def leer_datos_scraper_reciente(self):
        """
        Lee datos más recientes del scraper con MEJOR DETECCIÓN
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            hojas_disponibles = [worksheet.title for worksheet in spreadsheet.worksheets()]
            
            print(f"HOJAS DISPONIBLES: {hojas_disponibles}")
            
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
            
            # Ordenar por fecha (más reciente primero)
            hojas_datos.sort(key=lambda x: x[1], reverse=True)
            hoja_mas_reciente, fecha_obj, fecha_str = hojas_datos[0]
            
            print(f"LEYENDO: Datos más recientes desde {hoja_mas_reciente} ({fecha_str})")
            
            # Leer la hoja más reciente
            worksheet = spreadsheet.worksheet(hoja_mas_reciente)
            data = worksheet.get_all_values()
            
            if not data:
                print(f"ERROR: Hoja {hoja_mas_reciente} está vacía")
                return None, None
            
            # Convertir a DataFrame
            headers = data[0]
            rows = data[1:]
            
            if not rows:
                print(f"ERROR: Hoja {hoja_mas_reciente} solo tiene headers")
                return None, None
            
            df = pd.DataFrame(rows, columns=headers)
            
            # Limpiar columnas numéricas
            df['Visitas'] = pd.to_numeric(df['Visitas'], errors='coerce').fillna(0).astype(int)
            df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce').fillna(0).astype(int)
            
            print(f"LEÍDO: {len(df)} motos desde {hoja_mas_reciente}")
            return df, fecha_str
            
        except Exception as e:
            print(f"ERROR LECTURA SCRAPER: {str(e)}")
            return None, None
    
    def guardar_historico_con_hojas_originales(self, df_historico, fecha_procesamiento):
        """
        Guarda el histórico en "Data_Historico" como especifica el usuario
        """
        try:
            print(f"[GUARDANDO] Histórico en Data_Historico...")
            
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
            
            # Subir datos principales
            headers = df_historico.columns.values.tolist()
            data_rows = df_historico.values.tolist()
            all_data = [headers] + data_rows
            
            # Usar batch update para mayor eficiencia
            worksheet_main.update(all_data)
            
            print(f"EXITO: Histórico actualizado en Data_Historico")
            print(f"COLUMNAS: {len(df_historico.columns)} columnas")
            print(f"MOTOS: {len(df_historico)} total")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            print(f"ERROR GUARDANDO HISTÓRICO: {str(e)}")
            return False

def test_google_sheets_motick():
    """Función de prueba MEJORADA con diagnóstico completo"""
    print("="*60)
    print("TEST GOOGLE SHEETS MOTICK - DIAGNÓSTICO COMPLETO")
    print("="*60)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    sheet_id = os.getenv('GOOGLE_SHEET_ID_MOTICK')
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
    
    print(f"SHEET_ID: {sheet_id}")
    print(f"CREDENCIALES: {'✅ Encontradas' if credentials_json else '❌ No encontradas'}")
    
    if not sheet_id:
        print("❌ ERROR: GOOGLE_SHEET_ID_MOTICK no configurado")
        return False
    
    try:
        if credentials_json:
            # Para GitHub Actions
            gs_handler = GoogleSheetsMotick(
                credentials_json_string=credentials_json,
                sheet_id=sheet_id
            )
        else:
            # Para testing local
            credentials_file = "../credentials/service-account.json"
            if not os.path.exists(credentials_file):
                print(f"❌ ERROR: No se encontró {credentials_file}")
                return False
            
            gs_handler = GoogleSheetsMotick(
                credentials_file=credentials_file,
                sheet_id=sheet_id
            )
        
        # Ejecutar test de conexión con diagnóstico completo
        return gs_handler.test_connection()
        
    except Exception as e:
        print(f"❌ ERROR EN TEST: {str(e)}")
        return False

if __name__ == "__main__":
    # Ejecutar test si se ejecuta directamente
    test_google_sheets_motick()
