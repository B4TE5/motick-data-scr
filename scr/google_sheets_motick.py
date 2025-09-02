"""
Google Sheets Handler para Motick Scraper
Maneja la subida y lectura de datos de motos desde/hacia Google Sheets
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
        Inicializar handler con credenciales
        
        Args:
            credentials_json_string: String JSON de credenciales (para GitHub Actions)
            sheet_id: ID del Google Sheet
            credentials_file: Ruta al archivo de credenciales (para testing local)
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
        """Probar conexion a Google Sheets"""
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            print(f"CONEXION: Exitosa al Sheet: {spreadsheet.title}")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            return True
        except Exception as e:
            print(f"ERROR CONEXION: {str(e)}")
            return False
    
    def crear_id_unico_real(self, fila):
        """
        Crea ID unico basado en: URL + cuenta + titulo + precio + km
        MISMA LOGICA QUE EL SISTEMA DE COCHES
        """
        try:
            url = str(fila.get('URL', '')).strip()
            cuenta = str(fila.get('Cuenta', '')).strip()
            titulo = str(fila.get('Titulo', '')).strip()
            precio = str(fila.get('Precio', '')).strip()
            km = str(fila.get('Kilometraje', '')).strip()
            
            # Normalizar titulo (quitar caracteres especiales)
            titulo_limpio = re.sub(r'[^\w\s]', '', titulo.lower())[:30]
            km_limpio = re.sub(r'[^\d]', '', km)
            precio_limpio = re.sub(r'[^\d]', '', precio)
            
            # Crear clave unica
            clave_unica = f"{url}_{cuenta}_{titulo_limpio}_{precio_limpio}_{km_limpio}"
            
            # Hash para ID manejable
            return hashlib.md5(clave_unica.encode()).hexdigest()[:12]
            
        except Exception as e:
            # Fallback con URL + timestamp
            url_safe = str(fila.get('URL', str(time.time())))
            return hashlib.md5(f"{url_safe}_{time.time()}".encode()).hexdigest()[:12]
    
    def subir_datos_scraper(self, df_motos, fecha_extraccion=None):
        """
        Sube los datos del scraper diario a Google Sheets
        
        Args:
            df_motos: DataFrame con datos de motos extraidas
            fecha_extraccion: Fecha de extraccion (formato DD/MM/YYYY)
        """
        try:
            if fecha_extraccion is None:
                fecha_extraccion = datetime.now().strftime("%d/%m/%Y")
            
            # Crear ID_Unico_Real para cada moto
            df_motos['ID_Unico_Real'] = df_motos.apply(self.crear_id_unico_real, axis=1)
            
            # Nombre de hoja basado en fecha
            sheet_name = f"Datos_{fecha_extraccion.replace('/', '_')}"
            
            print(f"\nSUBIENDO: Datos del scraper a hoja {sheet_name}")
            
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # Verificar si ya existe una hoja con este nombre
            try:
                existing_sheet = spreadsheet.worksheet(sheet_name)
                print(f"AVISO: Ya existe hoja {sheet_name} - sobrescribiendo")
                existing_sheet.clear()
                worksheet = existing_sheet
            except gspread.WorksheetNotFound:
                # Crear nueva hoja
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df_motos) + 10,
                    cols=len(df_motos.columns) + 2
                )
                print(f"CREANDO: Nueva hoja {sheet_name}")
            
            # Preparar datos para subir
            headers = df_motos.columns.values.tolist()
            data_rows = df_motos.values.tolist()
            all_data = [headers] + data_rows
            
            # Subir datos
            worksheet.update(all_data)
            
            print(f"EXITO: {len(df_motos)} motos subidas a {sheet_name}")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True, sheet_name
            
        except Exception as e:
            print(f"ERROR SUBIDA SCRAPER: {str(e)}")
            return False, None
    
    def leer_datos_historico(self, sheet_name="Data_Historico"):
        """
        Lee los datos del historico desde Google Sheets
        CORREGIDO: Lee desde "Data_Historico" como especifica el usuario
        
        Args:
            sheet_name: Nombre de la hoja del historico
            
        Returns:
            DataFrame con datos historicos o None si no existe
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                data = worksheet.get_all_values()
                
                if not data:
                    print(f"AVISO: Hoja {sheet_name} esta vacia")
                    return None
                
                # Convertir a DataFrame
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
        Lee los datos mas recientes del scraper desde Google Sheets
        
        Returns:
            DataFrame con datos del scraper mas reciente, fecha de extraccion
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            hojas_disponibles = [worksheet.title for worksheet in spreadsheet.worksheets()]
            
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
            
            # Convertir a DataFrame
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
    
    def guardar_historico_completo(self, df_historico, fecha_procesamiento, sheet_name="Historico_Motick"):
        """
        Guarda el historico completo actualizado a Google Sheets
        
        Args:
            df_historico: DataFrame con historico actualizado
            fecha_procesamiento: Fecha del procesamiento
            sheet_name: Nombre de la hoja del historico
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # Crear o actualizar hoja principal del historico
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                worksheet.clear()
                print(f"LIMPIANDO: Hoja existente {sheet_name}")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df_historico) + 20,
                    cols=len(df_historico.columns) + 5
                )
                print(f"CREANDO: Nueva hoja {sheet_name}")
            
            # Preparar datos para subir
            headers = df_historico.columns.values.tolist()
            data_rows = df_historico.values.tolist()
            all_data = [headers] + data_rows
            
            # Subir datos
            worksheet.update(all_data)
            
            # Crear hojas adicionales
            self.crear_hojas_resumen(spreadsheet, df_historico, fecha_procesamiento)
            
            print(f"EXITO: Historico actualizado en {sheet_name}")
            print(f"MOTOS: {len(df_historico)} total")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            print(f"ERROR GUARDANDO HISTORICO: {str(e)}")
            return False
    
    def guardar_historico_con_hojas_originales(self, df_historico, fecha_procesamiento):
        """
        Guarda el historico en "Data_Historico" como especifica el usuario
        Mantiene la misma lógica que el script local pero en Google Sheets
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # HOJA PRINCIPAL: Data_Historico (como especifica el usuario)
            try:
                worksheet_main = spreadsheet.worksheet("Data_Historico")
                worksheet_main.clear()
                print(f"LIMPIANDO: Hoja principal Data_Historico")
            except gspread.WorksheetNotFound:
                worksheet_main = spreadsheet.add_worksheet(
                    title="Data_Historico",
                    rows=len(df_historico) + 20,
                    cols=len(df_historico.columns) + 10
                )
                print(f"CREANDO: Nueva hoja Data_Historico")
            
            # Subir datos principales a Data_Historico
            headers = df_historico.columns.values.tolist()
            data_rows = df_historico.values.tolist()
            all_data = [headers] + data_rows
            worksheet_main.update(all_data)
            
            print(f"EXITO: Historico actualizado en Data_Historico")
            print(f"COLUMNAS: {len(df_historico.columns)} columnas (incluyendo Visitas/Likes por fechas)")
            print(f"MOTOS: {len(df_historico)} total")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            print(f"ERROR GUARDANDO EN DATA_HISTORICO: {str(e)}")
            return False
    
    def crear_hojas_resumen(self, spreadsheet, df_historico, fecha_procesamiento):
        """Crea hojas de resumen y estadisticas"""
        try:
            # HOJA: Resumen por cuenta
            try:
                columnas_visitas = [col for col in df_historico.columns if col.startswith('Visitas_')]
                columnas_likes = [col for col in df_historico.columns if col.startswith('Likes_')]
                
                if columnas_likes and columnas_visitas:
                    col_likes_reciente = columnas_likes[-1]
                    col_visitas_reciente = columnas_visitas[-1]
                    
                    # Asegurar que las columnas sean numericas
                    df_historico[col_likes_reciente] = pd.to_numeric(df_historico[col_likes_reciente], errors='coerce').fillna(0)
                    df_historico[col_visitas_reciente] = pd.to_numeric(df_historico[col_visitas_reciente], errors='coerce').fillna(0)
                    
                    resumen_cuenta = df_historico.groupby('Cuenta').agg({
                        'ID_Unico_Real': 'count',
                        col_visitas_reciente: ['sum', 'mean', 'max'],
                        col_likes_reciente: ['sum', 'mean', 'max']
                    }).round(2)
                    
                    resumen_cuenta.columns = [
                        'Total_Motos', 'Total_Visitas', 'Media_Visitas', 'Max_Visitas',
                        'Total_Likes', 'Media_Likes', 'Max_Likes'
                    ]
                    
                    # Subir resumen por cuenta
                    try:
                        ws_resumen = spreadsheet.worksheet("Resumen_Por_Cuenta")
                        ws_resumen.clear()
                    except gspread.WorksheetNotFound:
                        ws_resumen = spreadsheet.add_worksheet("Resumen_Por_Cuenta", rows=20, cols=10)
                    
                    # Convertir a lista para subir
                    resumen_data = [resumen_cuenta.columns.tolist()] + resumen_cuenta.values.tolist()
                    ws_resumen.update(resumen_data)
                    
                    print("CREADO: Resumen por cuenta")
            
            except Exception as e:
                print(f"ADVERTENCIA: Error creando resumen por cuenta: {str(e)}")
            
            # HOJA: Top motos (solo activas)
            try:
                motos_activas = df_historico[df_historico['Estado'] == 'activa'].copy()
                
                if not motos_activas.empty and columnas_likes:
                    col_likes_reciente = columnas_likes[-1]
                    motos_activas[col_likes_reciente] = pd.to_numeric(
                        motos_activas[col_likes_reciente], errors='coerce'
                    ).fillna(0)
                    
                    top_motos = motos_activas.nlargest(50, col_likes_reciente)
                    
                    try:
                        ws_top = spreadsheet.worksheet("Top_50_Likes")
                        ws_top.clear()
                    except gspread.WorksheetNotFound:
                        ws_top = spreadsheet.add_worksheet("Top_50_Likes", rows=60, cols=len(top_motos.columns))
                    
                    # Subir top motos
                    headers = top_motos.columns.values.tolist()
                    data_rows = top_motos.values.tolist()
                    top_data = [headers] + data_rows
                    ws_top.update(top_data)
                    
                    print("CREADO: Top 50 motos por likes")
            
            except Exception as e:
                print(f"ADVERTENCIA: Error creando top motos: {str(e)}")
            
            # HOJA: Motos vendidas
            try:
                motos_vendidas = df_historico[df_historico['Estado'] == 'vendida'].copy()
                
                if not motos_vendidas.empty:
                    motos_vendidas = motos_vendidas.sort_values('Fecha_Venta', ascending=False)
                    
                    try:
                        ws_vendidas = spreadsheet.worksheet("Motos_Vendidas")
                        ws_vendidas.clear()
                    except gspread.WorksheetNotFound:
                        ws_vendidas = spreadsheet.add_worksheet("Motos_Vendidas", rows=len(motos_vendidas)+10, cols=len(motos_vendidas.columns))
                    
                    # Subir motos vendidas
                    headers = motos_vendidas.columns.values.tolist()
                    data_rows = motos_vendidas.values.tolist()
                    vendidas_data = [headers] + data_rows
                    ws_vendidas.update(vendidas_data)
                    
                    print("CREADO: Historial de motos vendidas")
            
            except Exception as e:
                print(f"ADVERTENCIA: Error creando motos vendidas: {str(e)}")
                
        except Exception as e:
            print(f"ADVERTENCIA: Error general creando hojas resumen: {str(e)}")

def test_google_sheets_motick():
    """Funcion de prueba para verificar conexion MOTICK"""
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
            return True
        else:
            return False
            
    except Exception as e:
        print(f"ERROR EN PRUEBA: {str(e)}")
        return False

if __name__ == "__main__":
    # Ejecutar prueba si se ejecuta directamente
    test_google_sheets_motick()
