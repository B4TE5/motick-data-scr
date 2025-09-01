"""
Analizador Historico Motick - Version Google Sheets V8.0 CORREGIDO
Lee datos del scraper desde Google Sheets y actualiza el historico evolutivo

CORRECCIONES CRITICAS VERSION 8.0:
- URL como identificador unico principal (SIN PRECIO)
- Mantiene hojas originales: Motos_Activas y Motos_Vendidas
- Añade Visitas_Totales y Likes_Totales
- NO incluye precio en ID unico (puede cambiar)
- Basado en ANALISIS_MOTICK_V7.py mejorado
"""

import sys
import os
import time
import pandas as pd
import re
import hashlib
from datetime import datetime, timedelta
import numpy as np

# Importar modulos locales
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import GOOGLE_SHEET_ID_MOTICK
from google_sheets_motick import GoogleSheetsMotick

class AnalizadorHistoricoMotick:
    def __init__(self):
        self.tiempo_inicio = datetime.now()
        
        # Variables de fecha (se establecen despues)
        self.fecha_actual = None
        self.fecha_str = None
        self.fecha_display = None
        
        # Estadisticas de procesamiento
        self.stats = {
            'total_archivo_nuevo': 0,
            'total_historico': 0,
            'motos_nuevas': 0,
            'motos_actualizadas': 0,
            'motos_vendidas': 0,
            'errores': 0,
            'tiempo_ejecucion': 0
        }
        
        # Listas para tracking
        self.motos_nuevas_lista = []
        self.motos_vendidas_lista = []
        self.top_likes_crecimiento = []
        
        # Google Sheets handler
        self.gs_handler = None
        
    def inicializar_google_sheets(self):
        """Inicializa la conexion a Google Sheets"""
        try:
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            sheet_id = os.getenv('GOOGLE_SHEET_ID') or GOOGLE_SHEET_ID_MOTICK
            
            if not credentials_json:
                # Para testing local
                credentials_file = "../credentials/service-account.json"
                if os.path.exists(credentials_file):
                    self.gs_handler = GoogleSheetsMotick(
                        credentials_file=credentials_file,
                        sheet_id=sheet_id
                    )
                else:
                    raise Exception("No se encontraron credenciales locales")
            else:
                # Para GitHub Actions
                self.gs_handler = GoogleSheetsMotick(
                    credentials_json_string=credentials_json,
                    sheet_id=sheet_id
                )
            
            # Probar conexion
            if not self.gs_handler.test_connection():
                raise Exception("No se pudo conectar a Google Sheets")
            
            print("CONEXION: Google Sheets inicializada correctamente")
            return True
            
        except Exception as e:
            print(f"ERROR INICIALIZACION: {str(e)}")
            return False
    
    def crear_id_unico_real(self, fila):
        """
        Crea ID unico basado SOLO en: URL + cuenta + titulo + km
        CORRECCION CRITICA V8.0: NO INCLUYE PRECIO (puede cambiar)
        """
        try:
            url = str(fila.get('URL', '')).strip()
            cuenta = str(fila.get('Cuenta', '')).strip()
            titulo = str(fila.get('Titulo', '')).strip()
            km = str(fila.get('Kilometraje', '')).strip()
            
            # Normalizar titulo (quitar caracteres especiales)
            titulo_limpio = re.sub(r'[^\w\s]', '', titulo.lower())[:30]
            km_limpio = re.sub(r'[^\d]', '', km)
            
            # Crear clave unica SIN PRECIO
            clave_unica = f"{url}_{cuenta}_{titulo_limpio}_{km_limpio}"
            
            # Hash para ID manejable
            return hashlib.md5(clave_unica.encode()).hexdigest()[:12]
            
        except Exception as e:
            # Fallback con URL + timestamp
            url_safe = str(fila.get('URL', str(time.time())))
            return hashlib.md5(f"{url_safe}_{time.time()}".encode()).hexdigest()[:12]
    
    def extraer_fecha_de_datos(self, df_nuevo):
        """Extrae la fecha de los datos del scraper"""
        try:
            if 'Fecha_Extraccion' in df_nuevo.columns:
                fecha_str = df_nuevo['Fecha_Extraccion'].iloc[0]
                if isinstance(fecha_str, str) and '/' in fecha_str:
                    fecha_parte = fecha_str.split(' ')[0]  # Quitar hora si existe
                    fecha_obj = datetime.strptime(fecha_parte, "%d/%m/%Y")
                    return fecha_obj, fecha_parte
            
            # Fallback: usar fecha actual
            fecha_actual = datetime.now()
            fecha_display = fecha_actual.strftime("%d/%m/%Y")
            print(f"ADVERTENCIA: Usando fecha actual {fecha_display}")
            return fecha_actual, fecha_display
            
        except Exception as e:
            print(f"ADVERTENCIA: Error extrayendo fecha: {e}, usando fecha actual")
            fecha_actual = datetime.now()
            return fecha_actual, fecha_actual.strftime("%d/%m/%Y")
    
    def mostrar_header(self):
        """Muestra el header del sistema"""
        print("="*80)
        print("ANALIZADOR HISTORICO MOTICK V8.0 - VERSION GOOGLE SHEETS CORREGIDA")
        print("="*80)
        print(f"Fecha procesamiento: {self.fecha_display}")
        print("Logica: URL como identificador unico principal (SIN PRECIO)")
        print("Fuente: Google Sheets")
        print("Hojas: Motos_Activas, Motos_Vendidas")
        print()
        
    def normalizar_nombres_columnas(self, df):
        """Normaliza los nombres de columnas a los esperados"""
        mapeo_columnas = {
            'ID_Moto': 'ID_Real_Wallapop',
            'id_moto': 'ID_Real_Wallapop',
            'ID': 'ID_Real_Wallapop',
            'id': 'ID_Real_Wallapop',
            'titulo': 'Titulo',
            'TITULO': 'Titulo',
            'precio': 'Precio',
            'PRECIO': 'Precio',
            'ano': 'Ano',
            'Ano': 'Ano',
            'ANO': 'Ano',
            'year': 'Ano',
            'kilometraje': 'Kilometraje',
            'KILOMETRAJE': 'Kilometraje',
            'km': 'Kilometraje',
            'KM': 'Kilometraje',
            'visitas': 'Visitas',
            'VISITAS': 'Visitas',
            'views': 'Visitas',
            'VIEWS': 'Visitas',
            'likes': 'Likes',
            'LIKES': 'Likes',
            'url': 'URL',
            'Url': 'URL',
            'cuenta': 'Cuenta',
            'CUENTA': 'Cuenta',
            'account': 'Cuenta'
        }
        
        df = df.rename(columns=mapeo_columnas)
        return df
    
    def validar_estructura_archivo(self, df):
        """Valida que el archivo tenga la estructura esperada"""
        print(f"Columnas encontradas: {list(df.columns)}")
        
        df = self.normalizar_nombres_columnas(df)
        
        # Columnas criticas
        columnas_minimas = ['Titulo', 'Visitas', 'Likes', 'URL']
        columnas_faltantes = [col for col in columnas_minimas if col not in df.columns]
        
        if columnas_faltantes:
            raise ValueError(f"Columnas criticas faltantes: {columnas_faltantes}")
        
        # Agregar columnas faltantes con valores por defecto
        columnas_deseadas = {
            'Cuenta': 'No especificado',
            'Precio': 'No especificado', 
            'Ano': 'No especificado',
            'Kilometraje': 'No especificado'
        }
        
        for col, valor_default in columnas_deseadas.items():
            if col not in df.columns:
                df[col] = valor_default
                print(f"Columna '{col}' anadida con valor por defecto")
        
        # Limpiar URLs vacias
        urls_vacias = df['URL'].isnull().sum() + (df['URL'] == 'No especificado').sum()
        if urls_vacias > 0:
            print(f"ADVERTENCIA: {urls_vacias} motos con URL vacia seran ignoradas")
            df = df[df['URL'].notna()]
            df = df[df['URL'] != 'No especificado']
        
        return df
        
    def leer_datos_scraper(self):
        """Lee los datos mas recientes del scraper desde Google Sheets"""
        try:
            df_nuevo, fecha_str = self.gs_handler.leer_datos_scraper_reciente()
            
            if df_nuevo is None:
                raise Exception("No se encontraron datos del scraper en Google Sheets")
            
            df_nuevo = self.validar_estructura_archivo(df_nuevo)
            
            # Crear ID_Unico_Real para cada moto (SIN PRECIO)
            df_nuevo['ID_Unico_Real'] = df_nuevo.apply(self.crear_id_unico_real, axis=1)
            
            # Limpiar columnas numericas
            df_nuevo['Visitas'] = pd.to_numeric(df_nuevo['Visitas'], errors='coerce').fillna(0).astype(int)
            df_nuevo['Likes'] = pd.to_numeric(df_nuevo['Likes'], errors='coerce').fillna(0).astype(int)
            
            self.stats['total_archivo_nuevo'] = len(df_nuevo)
            print(f"Motos en datos del scraper: {self.stats['total_archivo_nuevo']:,}")
            
            # Extraer fecha
            self.fecha_actual, self.fecha_display = self.extraer_fecha_de_datos(df_nuevo)
            self.fecha_str = self.fecha_actual.strftime("%Y%m%d")
            
            return df_nuevo
            
        except Exception as e:
            print(f"ERROR leyendo datos del scraper: {str(e)}")
            raise
            
    def obtener_columnas_fechas(self, df):
        """Obtiene las columnas de visitas y likes por fecha del historico"""
        columnas_visitas = [col for col in df.columns if col.startswith('Visitas_') and not col.endswith('_Totales')]
        columnas_likes = [col for col in df.columns if col.startswith('Likes_') and not col.endswith('_Totales')]
        
        columnas_visitas.sort()
        columnas_likes.sort()
        return columnas_visitas, columnas_likes
    
    def obtener_fecha_anterior(self, columnas_fechas):
        """Obtiene la fecha anterior mas reciente de las columnas (excluyendo la fecha actual)"""
        if not columnas_fechas:
            return None
        
        fechas = []
        for col in columnas_fechas:
            try:
                fecha_str = col.split('_')[1]
                fecha_obj = datetime.strptime(fecha_str, "%d/%m/%Y")
                # IMPORTANTE: Excluir la fecha actual para evitar autocomparacion
                if fecha_obj.strftime("%d/%m/%Y") != self.fecha_display:
                    fechas.append(fecha_obj)
            except:
                continue
        
        if fechas:
            return max(fechas).strftime("%d/%m/%Y")
        return None
    
    def limpiar_columnas_numericas(self, df):
        """Limpia y convierte todas las columnas numericas a los tipos correctos"""
        try:
            print("Limpiando columnas numericas...")
            
            # Obtener todas las columnas de visitas y likes
            columnas_visitas, columnas_likes = self.obtener_columnas_fechas(df)
            
            # Limpiar columnas basicas
            columnas_numericas_basicas = ['Visitas', 'Likes', 'Variacion_Likes']
            for col in columnas_numericas_basicas:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
            # Limpiar columnas totales
            if 'Visitas_Totales' in df.columns:
                df['Visitas_Totales'] = pd.to_numeric(df['Visitas_Totales'], errors='coerce').fillna(0).astype(int)
            if 'Likes_Totales' in df.columns:
                df['Likes_Totales'] = pd.to_numeric(df['Likes_Totales'], errors='coerce').fillna(0).astype(int)
            
            # Limpiar TODAS las columnas de fechas (visitas y likes)
            todas_columnas_fechas = columnas_visitas + columnas_likes
            for col in todas_columnas_fechas:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                    
            print(f"Columnas numericas limpiadas: {len(todas_columnas_fechas)} columnas de fechas")
            return df
            
        except Exception as e:
            print(f"ADVERTENCIA: Error limpiando columnas: {str(e)}")
            return df
        
    def primera_ejecucion(self, df_nuevo):
        """Crea el historico por primera vez con formato de columnas por fecha"""
        print("Primera ejecucion - Creando historico inicial")
        
        df_historico = df_nuevo.copy()
        
        df_historico['Primera_Deteccion'] = self.fecha_display
        df_historico['Estado'] = 'activa'
        df_historico['Fecha_Venta'] = pd.NA
        
        col_visitas_hoy = f"Visitas_{self.fecha_display}"
        col_likes_hoy = f"Likes_{self.fecha_display}"
        
        df_historico[col_visitas_hoy] = df_historico['Visitas']
        df_historico[col_likes_hoy] = df_historico['Likes']
        
        # Agregar columnas totales
        df_historico['Visitas_Totales'] = df_historico['Visitas']
        df_historico['Likes_Totales'] = df_historico['Likes']
        
        df_historico = df_historico.drop(['Visitas', 'Likes'], axis=1)
        df_historico['Variacion_Likes'] = 0
        
        # Ordenar columnas correctamente
        columnas_orden = [
            'ID_Unico_Real', 'Cuenta', 'Titulo', 'Precio', 'Kilometraje',
            'Primera_Deteccion', 'Estado', 'Fecha_Venta', 'URL',
            'Visitas_Totales', 'Likes_Totales',
            col_visitas_hoy, col_likes_hoy, 'Variacion_Likes'
        ]
        
        # Solo usar columnas que existen
        columnas_existentes = [col for col in columnas_orden if col in df_historico.columns]
        df_historico = df_historico[columnas_existentes]
        
        self.stats['total_historico'] = len(df_historico)
        self.stats['motos_nuevas'] = len(df_historico)
        self.motos_nuevas_lista = df_historico[['Titulo', 'Cuenta', 'Precio']].to_dict('records')
        
        return df_historico
        
    def leer_historico_existente(self):
        """Lee el historico existente desde Google Sheets"""
        try:
            df_historico = self.gs_handler.leer_datos_historico()
            
            if df_historico is None:
                print("AVISO: No existe historico previo - sera creado en primera ejecucion")
                return None
            
            # Verificar que tiene las columnas necesarias
            columnas_requeridas = ['URL', 'Estado']
            columnas_faltantes = [col for col in columnas_requeridas if col not in df_historico.columns]
            if columnas_faltantes:
                raise Exception(f"Columnas faltantes en historico: {columnas_faltantes}")
            
            self.stats['total_historico'] = len(df_historico)
            print(f"Motos en historico: {self.stats['total_historico']:,}")
            
            # Limpiar columnas numericas
            df_historico = self.limpiar_columnas_numericas(df_historico)
            
            return df_historico
                
        except Exception as e:
            print(f"ERROR leyendo historico: {str(e)}")
            raise
            
    def procesar_motos_nuevas_y_existentes(self, df_nuevo, df_historico):
        """
        LOGICA CORREGIDA V8.0: USA URLs como identificador principal
        NO INCLUYE PRECIO EN COMPARACIONES
        """
        
        try:
            col_visitas_hoy = f"Visitas_{self.fecha_display}"
            col_likes_hoy = f"Likes_{self.fecha_display}"
            
            print(f"Anadiendo columnas: {col_visitas_hoy}, {col_likes_hoy}")
            
            columnas_visitas, columnas_likes = self.obtener_columnas_fechas(df_historico)
            fecha_anterior = self.obtener_fecha_anterior(columnas_visitas)
            
            print(f"Fechas anteriores detectadas: {len(columnas_visitas)} dias de datos")
            if fecha_anterior:
                print(f"Comparando con: {fecha_anterior}")
            
            # Verificar si las columnas de hoy ya existen
            if col_visitas_hoy in df_historico.columns or col_likes_hoy in df_historico.columns:
                print(f"ADVERTENCIA: Las columnas para {self.fecha_display} ya existen")
                print("Se sobrescribiran los datos existentes")
            
            # URL COMO IDENTIFICADOR PRINCIPAL
            urls_historico = set(df_historico['URL'].values)
            urls_nuevos = set(df_nuevo['URL'].values)
            
            motos_nuevas_urls = urls_nuevos - urls_historico
            motos_existentes_urls = urls_nuevos & urls_historico
            motos_vendidas_urls = urls_historico - urls_nuevos
            
            print(f"Analisis de cambios:")
            print(f"    • Nuevas: {len(motos_nuevas_urls)}")
            print(f"    • Existentes: {len(motos_existentes_urls)}")
            print(f"    • Posibles ventas: {len(motos_vendidas_urls)}")
            
            df_actualizado = df_historico.copy()
            df_actualizado[col_visitas_hoy] = pd.NA
            df_actualizado[col_likes_hoy] = pd.NA
            
            # PROCESAR MOTOS EXISTENTES (USAR URL)
            for url_moto in motos_existentes_urls:
                try:
                    fila_nueva = df_nuevo[df_nuevo['URL'] == url_moto].iloc[0]
                    visitas_nuevas = int(fila_nueva['Visitas']) if pd.notna(fila_nueva['Visitas']) else 0
                    likes_nuevos = int(fila_nueva['Likes']) if pd.notna(fila_nueva['Likes']) else 0
                    
                    mask = df_actualizado['URL'] == url_moto
                    df_actualizado.loc[mask, col_visitas_hoy] = visitas_nuevas
                    df_actualizado.loc[mask, col_likes_hoy] = likes_nuevos
                    df_actualizado.loc[mask, 'Estado'] = 'activa'
                    
                    # Actualizar totales
                    if 'Visitas_Totales' in df_actualizado.columns:
                        df_actualizado.loc[mask, 'Visitas_Totales'] = visitas_nuevas
                    if 'Likes_Totales' in df_actualizado.columns:
                        df_actualizado.loc[mask, 'Likes_Totales'] = likes_nuevos
                    
                    self.stats['motos_actualizadas'] += 1
                    
                    # Calcular variacion de likes respecto a fecha anterior
                    if fecha_anterior:
                        col_likes_anterior = f"Likes_{fecha_anterior}"
                        if col_likes_anterior in df_actualizado.columns:
                            try:
                                likes_anteriores = df_actualizado.loc[mask, col_likes_anterior].iloc[0]
                                if pd.notna(likes_anteriores):
                                    likes_anteriores = int(likes_anteriores)
                                    variacion_likes = likes_nuevos - likes_anteriores
                                    df_actualizado.loc[mask, 'Variacion_Likes'] = variacion_likes
                                    
                                    if variacion_likes > 3:
                                        self.top_likes_crecimiento.append({
                                            'Titulo': fila_nueva['Titulo'],
                                            'Cuenta': fila_nueva['Cuenta'],
                                            'Variacion': variacion_likes,
                                            'Likes_Anteriores': likes_anteriores,
                                            'Likes_Nuevos': likes_nuevos
                                        })
                            except:
                                df_actualizado.loc[mask, 'Variacion_Likes'] = 0
                        
                except Exception as e:
                    self.stats['errores'] += 1
                    print(f"Error procesando moto existente: {str(e)}")
                    continue
            
            # PROCESAR MOTOS VENDIDAS (USAR URL)
            for url_moto in motos_vendidas_urls:
                try:
                    mask = df_actualizado['URL'] == url_moto
                    if mask.any():
                        estado_actual = df_actualizado.loc[mask, 'Estado'].iloc[0]
                        
                        if estado_actual == 'activa':
                            df_actualizado.loc[mask, 'Estado'] = 'vendida'
                            df_actualizado.loc[mask, 'Fecha_Venta'] = self.fecha_display
                            
                            fila_vendida = df_actualizado.loc[mask].iloc[0]
                            
                            # Obtener visitas y likes de la fecha anterior para el reporte
                            visitas_anteriores = 0
                            likes_anteriores = 0
                            if fecha_anterior:
                                col_visitas_anterior = f"Visitas_{fecha_anterior}"
                                col_likes_anterior = f"Likes_{fecha_anterior}"
                                if col_visitas_anterior in df_actualizado.columns:
                                    visitas_anteriores = df_actualizado.loc[mask, col_visitas_anterior].iloc[0]
                                    if pd.isna(visitas_anteriores):
                                        visitas_anteriores = 0
                                if col_likes_anterior in df_actualizado.columns:
                                    likes_anteriores = df_actualizado.loc[mask, col_likes_anterior].iloc[0]
                                    if pd.isna(likes_anteriores):
                                        likes_anteriores = 0
                            
                            self.motos_vendidas_lista.append({
                                'Titulo': fila_vendida['Titulo'],
                                'Cuenta': fila_vendida['Cuenta'],
                                'Precio': fila_vendida['Precio'],
                                'Visitas': int(visitas_anteriores),
                                'Likes': int(likes_anteriores)
                            })
                            
                            self.stats['motos_vendidas'] += 1
                        
                except Exception as e:
                    print(f"Error procesando moto vendida: {str(e)}")
                    continue
            
            # PROCESAR MOTOS NUEVAS (USAR URL)
            for url_nueva in motos_nuevas_urls:
                try:
                    fila_nueva = df_nuevo[df_nuevo['URL'] == url_nueva].iloc[0]
                    
                    nueva_fila = {
                        'ID_Unico_Real': fila_nueva['ID_Unico_Real'],
                        'Cuenta': str(fila_nueva['Cuenta']) if pd.notna(fila_nueva['Cuenta']) else 'No especificado',
                        'Titulo': str(fila_nueva['Titulo']) if pd.notna(fila_nueva['Titulo']) else 'No especificado',
                        'Precio': str(fila_nueva['Precio']) if pd.notna(fila_nueva['Precio']) else 'No especificado',
                        'Kilometraje': str(fila_nueva['Kilometraje']) if pd.notna(fila_nueva['Kilometraje']) else 'No especificado',
                        'Primera_Deteccion': self.fecha_display,
                        'Estado': 'activa',
                        'Fecha_Venta': pd.NA,
                        'URL': str(fila_nueva['URL']),
                        'Variacion_Likes': 0
                    }
                    
                    # Valores iniciales para totales
                    visitas_iniciales = int(fila_nueva['Visitas']) if pd.notna(fila_nueva['Visitas']) else 0
                    likes_iniciales = int(fila_nueva['Likes']) if pd.notna(fila_nueva['Likes']) else 0
                    
                    nueva_fila['Visitas_Totales'] = visitas_iniciales
                    nueva_fila['Likes_Totales'] = likes_iniciales
                    
                    # Inicializar todas las columnas de fechas anteriores con NA
                    for col_visitas in columnas_visitas:
                        nueva_fila[col_visitas] = pd.NA
                    for col_likes in columnas_likes:
                        nueva_fila[col_likes] = pd.NA
                    
                    # Anadir datos para la fecha actual
                    nueva_fila[col_visitas_hoy] = visitas_iniciales
                    nueva_fila[col_likes_hoy] = likes_iniciales
                    
                    df_actualizado = pd.concat([df_actualizado, pd.DataFrame([nueva_fila])], ignore_index=True)
                    
                    self.stats['motos_nuevas'] += 1
                    self.motos_nuevas_lista.append({
                        'Titulo': nueva_fila['Titulo'],
                        'Cuenta': nueva_fila['Cuenta'],
                        'Precio': nueva_fila['Precio']
                    })
                    
                except Exception as e:
                    self.stats['errores'] += 1
                    print(f"Error procesando moto nueva: {str(e)}")
                    continue
            
            # LIMPIEZA FINAL
            print("Limpieza final de datos...")
            df_actualizado = self.limpiar_columnas_numericas(df_actualizado)
            
            # ORDENACION: activas arriba por likes, vendidas abajo
            df_activas = df_actualizado[df_actualizado['Estado'] == 'activa'].copy()
            df_vendidas = df_actualizado[df_actualizado['Estado'] == 'vendida'].copy()
            
            # Ordenar activas por likes y visitas de hoy
            if col_likes_hoy in df_activas.columns:
                df_activas = df_activas.sort_values(
                    [col_likes_hoy, col_visitas_hoy], ascending=[False, False], na_position='last'
                )
            
            # Ordenar vendidas por fecha de venta
            if not df_vendidas.empty and 'Fecha_Venta' in df_vendidas.columns:
                df_vendidas = df_vendidas.sort_values('Fecha_Venta', ascending=False, na_position='last')
            
            # Concatenar: activas arriba, vendidas abajo
            df_actualizado = pd.concat([df_activas, df_vendidas], ignore_index=True)
            
            return df_actualizado
            
        except Exception as e:
            print(f"ERROR critico en procesamiento: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
            
    def mostrar_resumen_final(self):
        """Muestra el resumen final"""
        tiempo_total = (datetime.now() - self.tiempo_inicio).total_seconds()
        self.stats['tiempo_ejecucion'] = tiempo_total
        
        print(f"\n{'='*80}")
        print("PROCESAMIENTO COMPLETADO V8.0 CORREGIDO")
        print("="*80)
        print(f"Fecha procesada: {self.fecha_display}")
        print(f"Motos en datos del scraper: {self.stats['total_archivo_nuevo']:,}")
        print(f"Motos en historico: {self.stats['total_historico']:,}")
        print(f"Motos nuevas detectadas: {self.stats['motos_nuevas']:,}")
        print(f"Motos actualizadas: {self.stats['motos_actualizadas']:,}")
        print(f"Motos vendidas: {self.stats['motos_vendidas']:,}")
        print(f"Errores procesamiento: {self.stats['errores']:,}")
        print(f"Tiempo ejecucion: {tiempo_total:.2f} segundos")
        print(f"Nuevas columnas: Visitas_{self.fecha_display}, Likes_{self.fecha_display}")
        
        if self.top_likes_crecimiento:
            print(f"\nDESTACADOS DEL DIA:")
            top_likes = self.top_likes_crecimiento[0]
            print(f"Mayor crecimiento likes: +{top_likes['Variacion']} | {top_likes['Titulo'][:40]}")
        
        if self.stats['errores'] > 0:
            print(f"\nADVERTENCIAS:")
            print(f"Se produjeron {self.stats['errores']} errores durante el procesamiento")
            print("Las motos con errores mantuvieron sus datos anteriores")
        
        print("\nHistorico evolutivo consolidado exitosamente!")
        print("LOGICA CORREGIDA V8.0: URL como identificador unico (SIN PRECIO)")
        print("Hojas: Historico_Motick, Motos_Activas, Motos_Vendidas")
        
    def ejecutar(self):
        """Funcion principal que ejecuta todo el proceso"""
        try:
            # 1. Inicializar Google Sheets
            if not self.inicializar_google_sheets():
                return False
            
            # 2. Leer datos del scraper mas reciente
            df_nuevo = self.leer_datos_scraper()
            
            # 3. Mostrar header con fecha correcta
            self.mostrar_header()
            
            # 4. Procesar segun si es primera vez o no
            df_historico_existente = self.leer_historico_existente()
            
            if df_historico_existente is None:
                # Primera ejecucion
                df_historico_final = self.primera_ejecucion(df_nuevo)
            else:
                # Actualizar historico existente
                df_historico_final = self.procesar_motos_nuevas_y_existentes(df_nuevo, df_historico_existente)
                
            self.stats['total_historico'] = len(df_historico_final)
            
            # 5. Guardar historico actualizado en Google Sheets CON HOJAS ORIGINALES
            print("\nGuardando historico actualizado en Google Sheets...")
            success = self.gs_handler.guardar_historico_con_hojas_originales(
                df_historico_final, 
                self.fecha_display
            )
            
            if not success:
                print("ERROR: No se pudo guardar el historico en Google Sheets")
                return False
            
            # 6. Mostrar resumen final
            self.mostrar_resumen_final()
            return True
            
        except Exception as e:
            error_msg = str(e)
            print(f"\nERROR CRITICO: {error_msg}")
            print("El procesamiento no se pudo completar correctamente")
            self.stats['errores'] = 1
            
            if self.stats['motos_nuevas'] > 0 or self.stats['motos_actualizadas'] > 0:
                print(f"\nESTADISTICAS PARCIALES:")
                print(f"Nuevas procesadas: {self.stats['motos_nuevas']}")
                print(f"Actualizadas: {self.stats['motos_actualizadas']}")
                print(f"Vendidas: {self.stats['motos_vendidas']}")
            
            print(f"\nINFORMACION DE DEBUG:")
            import traceback
            traceback.print_exc()
            
            return False

def main():
    """Funcion principal del analizador"""
    print("Iniciando Analizador Historico MOTICK V8.0 - Version Google Sheets CORREGIDA...")
    print("VERSION AUTOMATIZADA V8.0:")
    print("   • Lee datos del scraper desde Google Sheets")
    print("   • Actualiza historico evolutivo en Google Sheets")
    print("   • USA URL como identificador unico principal (SIN PRECIO)")
    print("   • Mantiene hojas originales: Motos_Activas, Motos_Vendidas")
    print("   • Cada ejecucion anade: Visitas_FECHA y Likes_FECHA")
    print("   • Añade Visitas_Totales y Likes_Totales")
    print("   • Primera vez: Crea historico completo")
    print("   • Siguientes: Anade columnas por fecha")
    print()
    
    analizador = AnalizadorHistoricoMotick()
    exito = analizador.ejecutar()
    
    if exito:
        print("\nPROCESO COMPLETADO EXITOSAMENTE V8.0")
        print("FORMATO DEL HISTORICO:")
        print("   • Columnas basicas: ID_Unico_Real, Cuenta, Titulo, Precio, etc.")
        print("   • Columnas totales: Visitas_Totales, Likes_Totales")
        print("   • Columnas por fecha: Visitas_DD/MM/YYYY, Likes_DD/MM/YYYY")
        print("   • Ultima columna: Variacion_Likes (respecto a fecha anterior)")
        print("   • ORDENACION: Activas arriba (por likes), vendidas abajo")
        print("   • ID UNICO: URL + cuenta + titulo + km (SIN PRECIO)")
        
        print("\nPARA LA SIGUIENTE EJECUCION:")
        print("   1. El scraper se ejecutara automaticamente")
        print("   2. Este analizador se ejecutara automaticamente despues")
        print("   3. Se anadiran automaticamente las nuevas columnas")
        print("   URL siempre usado como identificador unico!")
        print("   Hojas: Historico_Motick, Motos_Activas, Motos_Vendidas")
        
        return True
    else:
        print("\nProceso completado con errores")
        print("Revisa los mensajes de error anteriores para diagnosticar el problema")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)