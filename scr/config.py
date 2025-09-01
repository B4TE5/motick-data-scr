"""
Configuracion para Motick Scraper Automation
"""

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Google Sheets Configuration
GOOGLE_SHEET_ID_MOTICK = os.getenv('GOOGLE_SHEET_ID_MOTICK', '')

# Para testing local - RUTA CORREGIDA
LOCAL_CREDENTIALS_FILE = "../credentials/service-account.json"

# Para testing rapido - Solo 2 cuentas
MOTICK_ACCOUNTS_TEST = {
    "MOTICK.MA M.": "https://es.wallapop.com/user/motick-432763398",
    'MOTICK.BA B.': 'https://es.wallapop.com/user/motick-432499045'
}

# CUENTAS MOTICK COMPLETAS
MOTICK_ACCOUNTS_FULL = {
    'MOTICK.MA M.': 'https://es.wallapop.com/user/motick-432763398',
    'MOTICK-MA M.': 'https://es.wallapop.com/user/motickm-459455125',
    'MOTICK.SE S.': 'https://es.wallapop.com/user/jaimed-432757399',
    'MOTICK-SE S.': 'https://es.wallapop.com/user/sevillam-459507970',
    'MOTICK.VA V.': 'https://es.wallapop.com/user/carlosm-432759147',
    'MOTICK-VA V.': 'https://es.wallapop.com/user/vlc2motickwallapop-459335984',
    'MOTICK.NO N.': 'https://es.wallapop.com/user/motickn-433191765',
    'MOTICK-BI B.': 'https://es.wallapop.com/user/alicante2-459337788',
    'MOTICK.MU M.': 'https://es.wallapop.com/user/jaimev-453393496',
    'MOTICK.MAL M.': 'https://es.wallapop.com/user/motickm-434475757',
    'MOTICK-MAL M.': 'https://es.wallapop.com/user/motickm-459451156',
    'MOTICK.BA B.': 'https://es.wallapop.com/user/motick-432499045',
    'MOTICK-BA B.': 'https://es.wallapop.com/user/ysuwr-459336516'
}

def get_motick_accounts(test_mode=False):
    """
    Devuelve lista de cuentas MOTICK segun el modo
    
    Args:
        test_mode: Si esta en modo test
    """
    test_mode = test_mode or os.getenv('TEST_MODE', 'false').lower() == 'true'
    
    if test_mode:
        print("MODO TEST: Solo procesando 2 cuentas MOTICK")
        return MOTICK_ACCOUNTS_TEST
    else:
        print(f"MODO COMPLETO: Procesando {len(MOTICK_ACCOUNTS_FULL)} cuentas MOTICK")
        return MOTICK_ACCOUNTS_FULL