"""
Google Sheets Handler para Motick Scraper - ARREGLO DEFINITIVO
Corrige el problema de formato de hojas SCR vs Datos
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
            fecha_para_hoja = datetime.strptime(fecha_extraccion, "%d/%m/%Y").strftime("%d/%m/%y")
            sheet_name = f"SCR {fecha_para_hoja}"
            
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
    SOLUCION DEFINITIVA: Versión simplificada que debe funcionar
    """
    try:
        spreadsheet = self.client.open_by_key(self.sheet_id)
        hojas_disponibles = [worksheet.title for worksheet in spreadsheet.worksheets()]
        
        print(f"DEBUG: Hojas disponibles: {hojas_disponibles}")
        
        # Filtrar solo hojas SCR y obtener sus fechas
        hojas_scr = []
        for hoja in hojas_disponibles:
            if hoja.startswith('SCR '):
                print(f"DEBUG: Procesando hoja SCR: '{hoja}'")
                try:
                    # Extraer parte de fecha: "SCR 03/09/25" -> "03/09/25"
                    fecha_scr = hoja[4:]  # Quitar "SCR "
                    print(f"DEBUG: Fecha extraída: '{fecha_scr}'")
                    
                    # Dividir por /
                    partes = fecha_scr.split('/')
                    if len(partes) == 3:
                        dia, mes, ano_2d = partes
                        print(f"DEBUG: Partes - dia:{dia}, mes:{mes}, año:{ano_2d}")
                        
                        # Convertir año de 2 dígitos a 4 dígitos
                        ano_int = int(ano_2d)
                        if ano_int <= 30:  # 00-30 = 2000-2030
                            ano_completo = 2000 + ano_int
                        else:  # 31-99 = 1931-1999  
                            ano_completo = 1900 + ano_int
                        
                        print(f"DEBUG: Año completo calculado: {ano_completo}")
                        
                        # Crear fecha completa
                        fecha_completa = f"{dia}/{mes}/{ano_completo}"
                        fecha_obj = datetime.strptime(fecha_completa, "%d/%m/%Y")
                        
                        hojas_scr.append({
                            'nombre': hoja,
                            'fecha_obj': fecha_obj, 
                            'fecha_str': fecha_completa
                        })
                        print(f"SUCCESS: '{hoja}' -> {fecha_completa}")
                        
                except Exception as e:
                    print(f"ERROR: Procesando '{hoja}': {str(e)}")
                    continue
        
        print(f"DEBUG: Total hojas SCR válidas encontradas: {len(hojas_scr)}")
        
        if not hojas_scr:
            print("ERROR: No se encontraron hojas SCR válidas")
            # Listar todas las hojas SCR para debug
            scr_hojas = [h for h in hojas_disponibles if h.startswith('SCR ')]
            print(f"DEBUG: Hojas SCR detectadas: {scr_hojas}")
            return None, None
        
        # Ordenar por fecha (más reciente primero)
        hojas_scr.sort(key=lambda x: x['fecha_obj'], reverse=True)
        hoja_reciente = hojas_scr[0]
        
        print(f"LEYENDO: Hoja más reciente '{hoja_reciente['nombre']}' ({hoja_reciente['fecha_str']})")
        
        # Leer datos de la hoja
        worksheet = spreadsheet.worksheet(hoja_reciente['nombre'])
        data = worksheet.get_all_values()
        
        if not data or len(data) < 2:
            print(f"ERROR: Hoja '{hoja_reciente['nombre']}' vacía o sin datos")
            return None, None
        
        # Crear DataFrame
        headers = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=headers)
        
        print(f"SUCCESS: Leídos {len(df)} registros desde '{hoja_reciente['nombre']}'")
        print(f"DEBUG: Columnas encontradas: {list(df.columns)[:5]}...")  # Mostrar primeras 5 columnas
        
        # Limpiar columnas numéricas básicas
        if 'Visitas' in df.columns:
            df['Visitas'] = pd.to_numeric(df['Visitas'], errors='coerce').fillna(0).astype(int)
        if 'Likes' in df.columns:
            df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce').fillna(0).astype(int)
        
        return df, hoja_reciente['fecha_str']
        
    except Exception as e:
        print(f"ERROR CRÍTICO en leer_datos_scraper_reciente: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None
    
    def guardar_historico_con_hojas_originales(self, df_historico, fecha_procesamiento):
        """
        VERSIÓN COMPLETA: Guarda en 3 hojas como requiere el usuario
        1. Data_Historico (todas las motos ordenadas)
        2. Motos_Activas (solo activas por Likes_Totales DESC)
        3. Motos_Vendidas (solo vendidas por Fecha_Venta DESC)
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # ===============================================
            # 1. HOJA PRINCIPAL: Data_Historico
            # ===============================================
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
            
            # Ordenar historico completo: Activas por Likes_Totales, Vendidas por Fecha_Venta
            df_ordenado = self.ordenar_historico_completo(df_historico)
            
            # Subir datos principales
            headers = df_ordenado.columns.values.tolist()
            data_rows = df_ordenado.values.tolist()
            all_data = [headers] + data_rows
            worksheet_main.update(all_data)
            
            print(f"EXITO: Data_Historico actualizada con {len(df_ordenado)} motos")
            
            # ===============================================
            # 2. HOJA MOTOS_ACTIVAS (solo activas por Likes_Totales DESC)
            # ===============================================
            motos_activas = df_historico[df_historico['Estado'] == 'activa'].copy()
            
            if not motos_activas.empty:
                # Ordenar por Likes_Totales descendente
                if 'Likes_Totales' in motos_activas.columns:
                    motos_activas = motos_activas.sort_values('Likes_Totales', ascending=False, na_position='last')
                
                try:
                    ws_activas = spreadsheet.worksheet("Motos_Activas")
                    ws_activas.clear()
                    print(f"LIMPIANDO: Hoja Motos_Activas existente")
                except gspread.WorksheetNotFound:
                    ws_activas = spreadsheet.add_worksheet(
                        "Motos_Activas", 
                        rows=len(motos_activas)+20, 
                        cols=len(motos_activas.columns)+5
                    )
                    print(f"CREANDO: Nueva hoja Motos_Activas")
                
                # Subir motos activas
                headers_activas = motos_activas.columns.values.tolist()
                data_activas = motos_activas.values.tolist()
                activas_data = [headers_activas] + data_activas
                ws_activas.update(activas_data)
                
                print(f"EXITO: Motos_Activas actualizada con {len(motos_activas)} motos")
            else:
                print("AVISO: No hay motos activas")
            
            # ===============================================
            # 3. HOJA MOTOS_VENDIDAS (solo vendidas por Fecha_Venta DESC)
            # ===============================================
            motos_vendidas = df_historico[df_historico['Estado'] == 'vendida'].copy()
            
            if not motos_vendidas.empty:
                # Ordenar por Fecha_Venta descendente
                if 'Fecha_Venta' in motos_vendidas.columns:
                    motos_vendidas = motos_vendidas.sort_values('Fecha_Venta', ascending=False, na_position='last')
                
                try:
                    ws_vendidas = spreadsheet.worksheet("Motos_Vendidas")
                    ws_vendidas.clear()
                    print(f"LIMPIANDO: Hoja Motos_Vendidas existente")
                except gspread.WorksheetNotFound:
                    ws_vendidas = spreadsheet.add_worksheet(
                        "Motos_Vendidas", 
                        rows=len(motos_vendidas)+20, 
                        cols=len(motos_vendidas.columns)+5
                    )
                    print(f"CREANDO: Nueva hoja Motos_Vendidas")
                
                # Subir motos vendidas
                headers_vendidas = motos_vendidas.columns.values.tolist()
                data_vendidas = motos_vendidas.values.tolist()
                vendidas_data = [headers_vendidas] + data_vendidas
                ws_vendidas.update(vendidas_data)
                
                print(f"EXITO: Motos_Vendidas actualizada con {len(motos_vendidas)} motos")
            else:
                print("AVISO: No hay motos vendidas")
            
            print(f"DATOS FINALES: {len(df_historico)} filas x {len(df_historico.columns)} columnas")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True
            
        except Exception as e:
            print(f"ERROR GUARDANDO HISTORICO COMPLETO: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def ordenar_historico_completo(self, df_historico):
        """
        Ordena el historico: activas por Likes_Totales DESC, vendidas por Fecha_Venta DESC
        """
        try:
            # Separar activas y vendidas
            df_activas = df_historico[df_historico['Estado'] == 'activa'].copy()
            df_vendidas = df_historico[df_historico['Estado'] == 'vendida'].copy()
            
            # Ordenar activas por Likes_Totales descendente (NO por visitas)
            if not df_activas.empty and 'Likes_Totales' in df_activas.columns:
                df_activas = df_activas.sort_values('Likes_Totales', ascending=False, na_position='last')
                print(f"ORDENACION: {len(df_activas)} activas ordenadas por Likes_Totales DESC")
            
            # Ordenar vendidas por Fecha_Venta descendente
            if not df_vendidas.empty and 'Fecha_Venta' in df_vendidas.columns:
                df_vendidas = df_vendidas.sort_values('Fecha_Venta', ascending=False, na_position='last')
                print(f"ORDENACION: {len(df_vendidas)} vendidas ordenadas por Fecha_Venta DESC")
            
            # Concatenar: activas arriba, vendidas abajo
            df_ordenado = pd.concat([df_activas, df_vendidas], ignore_index=True)
            
            return df_ordenado
            
        except Exception as e:
            print(f"ERROR ORDENANDO: {str(e)}")
            return df_historico

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
            
            # Probar lectura de datos scraper recientes
            print("\nPROBANDO LECTURA DE DATOS SCRAPER...")
            df_reciente, fecha = gs_handler.leer_datos_scraper_reciente()
            if df_reciente is not None:
                print(f"EXITO: Leidos {len(df_reciente)} registros del {fecha}")
                return True
            else:
                print("ERROR: No se pudieron leer datos del scraper")
                return False
        else:
            return False
            
    except Exception as e:
        print(f"ERROR EN PRUEBA: {str(e)}")
        return False

if __name__ == "__main__":
    test_google_sheets_motick()
