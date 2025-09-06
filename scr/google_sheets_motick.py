"""
Google Sheets Handler para Motick Scraper - CODIGO COMPLETO CORREGIDO
ARREGLO DEFINITIVO: Elimina error NAType not JSON serializable
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
            print(f"SHEET_ID PROBLEMATICO: {self.sheet_id}")
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
    
    def limpiar_dataframe_para_sheets(self, df):
        """
        FUNCION CRITICA: Limpia DataFrame para Google Sheets
        Elimina pd.NA, NaN, None y los convierte a strings vacios
        """
        try:
            # Crear copia para no modificar original
            df_clean = df.copy()
            
            # Reemplazar pd.NA y NaN con strings vacios
            df_clean = df_clean.fillna('')
            
            # Reemplazar cualquier pd.NA que quede
            df_clean = df_clean.replace({pd.NA: ''})
            
            # Convertir None a strings vacios
            df_clean = df_clean.replace({None: ''})
            
            # Asegurar que todo es serializable
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str)
                # Reemplazar 'nan', '<NA>', 'None' strings
                df_clean[col] = df_clean[col].replace({'nan': '', '<NA>': '', 'None': ''})
            
            print(f"LIMPIEZA: DataFrame limpiado para Google Sheets - {len(df_clean)} filas")
            return df_clean
            
        except Exception as e:
            print(f"ERROR EN LIMPIEZA: {str(e)}")
            return df
    
    def subir_datos_scraper(self, df_motos, fecha_extraccion=None):
        """
        Sube datos del scraper con limpieza de NA values
        """
        try:
            if fecha_extraccion is None:
                fecha_extraccion = datetime.now().strftime("%d/%m/%Y")
            
            # Crear ID_Unico_Real para cada moto
            df_motos['ID_Unico_Real'] = df_motos.apply(self.crear_id_unico_real, axis=1)
            
            # LIMPIAR DATAFRAME ANTES DE SUBIR
            df_motos_clean = self.limpiar_dataframe_para_sheets(df_motos)
            
            # Nombre de hoja basado en fecha
            fecha_para_hoja = datetime.strptime(fecha_extraccion, "%d/%m/%Y").strftime("%d/%m/%y")
            sheet_name = f"SCR {fecha_para_hoja}"
            
            print(f"SUBIENDO: Datos a hoja {sheet_name}")
            
            # Abrir Google Sheet
            spreadsheet = self.client.open_by_key(self.sheet_id)
            print(f"ACCEDIENDO: Sheet {spreadsheet.title}")
            
            # Crear o limpiar worksheet
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                worksheet.clear()
                print(f"LIMPIANDO: Hoja {sheet_name}")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=len(df_motos_clean) + 10,
                    cols=len(df_motos_clean.columns) + 2
                )
                print(f"CREANDO: Nueva hoja {sheet_name}")
            
            # Preparar datos para subir (YA LIMPIADOS)
            headers = df_motos_clean.columns.values.tolist()
            data_rows = df_motos_clean.values.tolist()
            all_data = [headers] + data_rows
            
            # Subir datos
            worksheet.update(all_data)
            
            print(f"SUBIDA EXITOSA: {sheet_name}")
            print(f"DATOS: {len(df_motos_clean)} filas x {len(df_motos_clean.columns)} columnas")
            print(f"URL: https://docs.google.com/spreadsheets/d/{self.sheet_id}")
            
            return True, sheet_name
            
        except Exception as e:
            print(f"ERROR SUBIDA: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, None
    
    def leer_datos_historico(self, sheet_name="Data_Historico"):
        """
        Lee datos del historico desde Google Sheets
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
        Lee los datos mas recientes del scraper desde hojas SCR
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            hojas_disponibles = [worksheet.title for worksheet in spreadsheet.worksheets()]
            
            print(f"DEBUG: Hojas disponibles: {hojas_disponibles}")
            
            # Buscar hojas SCR con manejo robusto de errores
            hojas_scr = []
            for hoja in hojas_disponibles:
                if hoja.startswith('SCR '):
                    print(f"DEBUG: Procesando hoja: {hoja}")
                    try:
                        fecha_parte = hoja[4:]  # "05/09/25"
                        print(f"DEBUG: Fecha extraida: '{fecha_parte}'")
                        
                        partes = fecha_parte.split('/')
                        print(f"DEBUG: Partes fecha: {partes}")
                        
                        if len(partes) == 3:
                            dia, mes, ano = partes
                            print(f"DEBUG: Parseando - Dia:{dia} Mes:{mes} Ano:{ano}")
                            
                            # Convertir ano de 2 digitos a 4 digitos
                            ano_int = int(ano)
                            if ano_int <= 30:  # 00-30 = 2000-2030
                                ano_completo = f"20{ano}"
                            else:  # 31-99 = 1931-1999
                                ano_completo = f"19{ano}"
                            
                            fecha_str = f"{dia}/{mes}/{ano_completo}"
                            print(f"DEBUG: Fecha completa: {fecha_str}")
                            
                            fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
                            hojas_scr.append((hoja, fecha_obj, fecha_str))
                            print(f"SCR detectada exitosamente: {hoja} -> {fecha_str}")
                        else:
                            print(f"DEBUG: Formato incorrecto, partes != 3: {len(partes)}")
                            
                    except Exception as e:
                        print(f"DEBUG: Error procesando {hoja}: {str(e)}")
                        continue
                else:
                    print(f"DEBUG: Ignorando hoja (no es SCR): {hoja}")
            
            print(f"DEBUG: Total hojas SCR encontradas: {len(hojas_scr)}")
            
            if not hojas_scr:
                print("ERROR: No se encontraron hojas SCR validas")
                print(f"DEBUG: Se buscaron hojas que empiecen con 'SCR ' en: {hojas_disponibles}")
                return None, None
            
            # Tomar la mas reciente
            hojas_scr.sort(key=lambda x: x[1], reverse=True)
            hoja_reciente = hojas_scr[0]
            
            print(f"DEBUG: Hoja mas reciente seleccionada: {hoja_reciente[0]} ({hoja_reciente[2]})")
            
            # Leer datos de la hoja mas reciente
            worksheet = spreadsheet.worksheet(hoja_reciente[0])
            data = worksheet.get_all_values()
            
            print(f"DEBUG: Datos brutos obtenidos: {len(data)} filas")
            
            if len(data) < 2:
                print("ERROR: Hoja sin datos suficientes")
                return None, None
            
            # Crear DataFrame
            headers = data[0]
            rows = data[1:]
            df = pd.DataFrame(rows, columns=headers)
            
            print(f"DEBUG: DataFrame creado: {len(df)} filas x {len(df.columns)} columnas")
            print(f"DEBUG: Columnas: {list(df.columns)}")
            
            # Limpiar columnas numericas
            if 'Visitas' in df.columns:
                df['Visitas'] = pd.to_numeric(df['Visitas'], errors='coerce').fillna(0).astype(int)
            if 'Likes' in df.columns:
                df['Likes'] = pd.to_numeric(df['Likes'], errors='coerce').fillna(0).astype(int)
            
            print(f"EXITO: {len(df)} motos leidas desde {hoja_reciente[0]}")
            return df, hoja_reciente[2]  # Devolver DataFrame y fecha_str
            
        except Exception as e:
            print(f"ERROR CRITICO en leer_datos_scraper_reciente: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def guardar_historico_con_hojas_originales(self, df_historico, fecha_procesamiento):
        """
        CORREGIDO: Guarda historico con limpieza completa de NA values
        Guarda en 3 hojas: Data_Historico, Motos_Activas, Motos_Vendidas
        """
        try:
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            print(f"INICIANDO GUARDADO: {len(df_historico)} motos total")
            
            # ===============================================
            # 1. HOJA PRINCIPAL: Data_Historico
            # ===============================================
            print("PROCESANDO: Hoja principal Data_Historico")
            
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
            
            # Ordenar historico completo
            df_ordenado = self.ordenar_historico_completo(df_historico)
            
            # LIMPIEZA CRITICA: Eliminar NA values antes de subir
            print("LIMPIANDO: Datos para Data_Historico")
            df_ordenado_clean = self.limpiar_dataframe_para_sheets(df_ordenado)
            
            # Preparar y subir datos principales
            headers = df_ordenado_clean.columns.values.tolist()
            data_rows = df_ordenado_clean.values.tolist()
            all_data = [headers] + data_rows
            
            print(f"SUBIENDO: {len(all_data)} filas a Data_Historico")
            worksheet_main.update(all_data)
            
            print(f"EXITO: Data_Historico actualizada con {len(df_ordenado)} motos")
            
            # ===============================================
            # 2. HOJA MOTOS_ACTIVAS (solo activas)
            # ===============================================
            print("PROCESANDO: Hoja Motos_Activas")
            
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
                
                # LIMPIEZA CRITICA: Eliminar NA values
                print("LIMPIANDO: Datos para Motos_Activas")
                motos_activas_clean = self.limpiar_dataframe_para_sheets(motos_activas)
                
                # Preparar y subir motos activas
                headers_activas = motos_activas_clean.columns.values.tolist()
                data_activas = motos_activas_clean.values.tolist()
                activas_data = [headers_activas] + data_activas
                
                print(f"SUBIENDO: {len(activas_data)} filas a Motos_Activas")
                ws_activas.update(activas_data)
                
                print(f"EXITO: Motos_Activas actualizada con {len(motos_activas)} motos")
            else:
                print("AVISO: No hay motos activas")
            
            # ===============================================
            # 3. HOJA MOTOS_VENDIDAS (solo vendidas)
            # ===============================================
            print("PROCESANDO: Hoja Motos_Vendidas")
            
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
                
                # LIMPIEZA CRITICA: Eliminar NA values
                print("LIMPIANDO: Datos para Motos_Vendidas")
                motos_vendidas_clean = self.limpiar_dataframe_para_sheets(motos_vendidas)
                
                # Preparar y subir motos vendidas
                headers_vendidas = motos_vendidas_clean.columns.values.tolist()
                data_vendidas = motos_vendidas_clean.values.tolist()
                vendidas_data = [headers_vendidas] + data_vendidas
                
                print(f"SUBIENDO: {len(vendidas_data)} filas a Motos_Vendidas")
                ws_vendidas.update(vendidas_data)
                
                print(f"EXITO: Motos_Vendidas actualizada con {len(motos_vendidas)} motos")
            else:
                print("AVISO: No hay motos vendidas")
            
            print(f"GUARDADO COMPLETO EXITOSO")
            print(f"DATOS FINALES: {len(df_historico)} filas x {len(df_historico.columns)} columnas")
            print(f"HOJAS ACTUALIZADAS: Data_Historico, Motos_Activas, Motos_Vendidas")
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
            
            # Ordenar activas por Likes_Totales descendente
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
    """Funcion de prueba para verificar conexion"""
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
