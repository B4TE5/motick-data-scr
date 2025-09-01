#!/usr/bin/env python3
"""
Setup Script para Motick Data Scraper
Configura el entorno local de desarrollo y testing
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def print_step(step_num, description):
    """Imprime paso del setup con formato consistente"""
    print(f"\n[STEP {step_num}] {description}")
    print("-" * 50)

def create_directories():
    """Crea las carpetas necesarias"""
    directories = [
        'src',
        'credentials', 
        'logs',
        'backup',
        '.github/workflows'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Creado directorio: {directory}")

def install_dependencies():
    """Instala las dependencias de Python"""
    print("Instalando dependencias de Python...")
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'], check=True)
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], check=True)
        print("Dependencias instaladas correctamente")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error instalando dependencias: {e}")
        return False

def create_env_file():
    """Crea archivo .env desde template"""
    env_template_path = '.env.template'
    env_path = '.env'
    
    if os.path.exists(env_path):
        response = input(f"El archivo {env_path} ya existe. ¿Sobrescribir? (y/N): ")
        if response.lower() != 'y':
            print("Manteniendo archivo .env existente")
            return True
    
    if os.path.exists(env_template_path):
        with open(env_template_path, 'r') as template:
            content = template.read()
        
        with open(env_path, 'w') as env_file:
            env_file.write(content)
        
        print(f"Archivo {env_path} creado desde template")
        print("IMPORTANTE: Edita .env con tus valores reales")
        return True
    else:
        print(f"ERROR: No se encontro {env_template_path}")
        return False

def setup_git_hooks():
    """Configura git hooks para evitar subir credenciales"""
    hooks_dir = Path('.git/hooks')
    if not hooks_dir.exists():
        print("No es un repositorio Git, saltando git hooks")
        return True
    
    pre_commit_hook = hooks_dir / 'pre-commit'
    
    hook_content = '''#!/bin/bash
# Git hook para evitar subir archivos sensibles

# Verificar que no se suban archivos de credenciales
if git diff --cached --name-only | grep -E "(\.env$|service-account\.json|credentials\.json)"; then
    echo "ERROR: Intentando subir archivos sensibles!"
    echo "Los siguientes archivos contienen credenciales y no deben subirse:"
    git diff --cached --name-only | grep -E "(\.env$|service-account\.json|credentials\.json)"
    echo "Usa git reset HEAD <archivo> para quitarlos del commit"
    exit 1
fi
'''
    
    with open(pre_commit_hook, 'w') as f:
        f.write(hook_content)
    
    # Hacer ejecutable
    os.chmod(pre_commit_hook, 0o755)
    print("Git hook pre-commit configurado para proteger credenciales")
    return True

def create_gitignore():
    """Crea o actualiza .gitignore"""
    gitignore_content = '''# Motick Data Scraper - Git Ignore

# Archivos de credenciales (NUNCA subir)
.env
*.json
credentials/
service-account*.json
google-credentials*.json

# Logs y datos temporales
logs/
*.log
backup/
temp/
cache/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/
.venv/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Selenium
geckodriver.log
chromedriver.log

# Datos sensibles
resultados_motick/
*.xlsx
*.csv
'''

    with open('.gitignore', 'w') as f:
        f.write(gitignore_content)
    
    print(".gitignore creado/actualizado")

def test_python_version():
    """Verifica version de Python"""
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"Python {version.major}.{version.minor}.{version.micro} - Compatible")
        return True
    else:
        print(f"ERROR: Python {version.major}.{version.minor}.{version.micro} no es compatible")
        print("Necesitas Python 3.8 o superior")
        return False

def create_sample_credentials_structure():
    """Crea estructura de ejemplo para credenciales"""
    credentials_dir = Path('credentials')
    readme_path = credentials_dir / 'README.md'
    
    readme_content = '''# Credentials Directory

Este directorio debe contener tus credenciales de Google Cloud:

## Para desarrollo local:

1. `service-account.json` - Archivo de cuenta de servicio de Google Cloud

## Cómo obtener credenciales:

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Crea un proyecto o selecciona uno existente
3. Habilita Google Sheets API
4. Ve a "Credenciales" > "Crear Credenciales" > "Cuenta de servicio"
5. Descarga el archivo JSON y renómbralo como `service-account.json`
6. Colócalo en este directorio

## IMPORTANTE:
- Estos archivos NUNCA se suben a GitHub (están en .gitignore)
- Para GitHub Actions, usa GitHub Secrets
- No compartas estos archivos públicamente

## Para Google Sheets:
1. Comparte tu Google Sheet con el email de la cuenta de servicio
2. Dale permisos de "Editor"
3. Copia el ID del Sheet (está en la URL)
'''
    
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    
    print("Estructura de credenciales creada con documentación")

def run_basic_tests():
    """Ejecuta tests básicos del sistema"""
    print("Ejecutando tests básicos...")
    
    # Test 1: Importar módulos principales
    try:
        # Agregar src al path para imports
        src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')
        if src_path not in sys.path:
            sys.path.append(src_path)
        
        from config import get_motick_accounts
        print("✓ Configuración cargada correctamente")
    except Exception as e:
        print(f"✗ Error cargando configuración: {e}")
        return False
    
    # Test 2: Verificar cuentas MOTICK
    try:
        accounts = get_motick_accounts(test_mode=True)
        print(f"✓ {len(accounts)} cuentas MOTICK configuradas para testing")
    except Exception as e:
        print(f"✗ Error verificando cuentas: {e}")
        return False
    
    # Test 3: Verificar dependencias críticas
    critical_modules = ['selenium', 'pandas', 'gspread', 'tqdm']
    for module in critical_modules:
        try:
            __import__(module)
            print(f"✓ {module} disponible")
        except ImportError:
            print(f"✗ {module} no encontrado")
            return False
    
    return True

def main():
    """Función principal del setup"""
    print("="*60)
    print("MOTICK DATA SCRAPER - SETUP DE DESARROLLO")
    print("="*60)
    
    # Verificar Python
    print_step(1, "Verificando Python")
    if not test_python_version():
        sys.exit(1)
    
    # Crear directorios
    print_step(2, "Creando estructura de directorios")
    create_directories()
    
    # Instalar dependencias
    print_step(3, "Instalando dependencias")
    if not install_dependencies():
        sys.exit(1)
    
    # Crear archivo .env
    print_step(4, "Configurando variables de entorno")
    create_env_file()
    
    # Configurar Git
    print_step(5, "Configurando Git")
    create_gitignore()
    setup_git_hooks()
    
    # Crear estructura de credenciales
    print_step(6, "Configurando estructura de credenciales")
    create_sample_credentials_structure()
    
    # Tests básicos
    print_step(7, "Ejecutando tests básicos")
    if not run_basic_tests():
        print("\nALGUNOS TESTS FALLARON - Revisa los errores arriba")
    
    # Instrucciones finales
    print("\n" + "="*60)
    print("SETUP COMPLETADO")
    print("="*60)
    print("\nPróximos pasos:")
    print("1. Edita .env con tu GOOGLE_SHEET_ID_MOTICK")
    print("2. Coloca service-account.json en credentials/")
    print("3. Comparte tu Google Sheet con la cuenta de servicio")
    print("4. Ejecuta: cd src && python scraper_motick.py (modo test)")
    
    print("\nPara GitHub Actions:")
    print("1. Configura GOOGLE_CREDENTIALS_JSON en GitHub Secrets")
    print("2. Configura GOOGLE_SHEET_ID_MOTICK en GitHub Secrets")
    print("3. El sistema se ejecutará automáticamente cada día")
    
    print("\nComandos útiles:")
    print("- Testing local: cd src && TEST_MODE=true python scraper_motick.py")
    print("- Verificar config: cd src && python -c 'from config import *; print(get_motick_accounts())'")
    print("- Test Google Sheets: cd src && python -c 'from google_sheets_motick import test_google_sheets_motick; test_google_sheets_motick()'")
    
    print("\n¡Sistema listo para usar!")

if __name__ == "__main__":
    main()