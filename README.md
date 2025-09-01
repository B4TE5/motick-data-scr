
<div align="center">

# 🏍️ Motick Moto Scraper 🏍️

**Sistema automatizado de scraping y análisis de mercado de motos en Wallapop**

[![Build](https://img.shields.io/badge/Build-Passing-success)](../../actions)
[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://www.python.org/downloads/)
[![Selenium](https://img.shields.io/badge/Selenium-WebDriver-43B02A)](https://www.selenium.dev/downloads/)
[![Google Sheets API](https://img.shields.io/badge/Google-Workspace-4285F4)](https://developers.google.com/workspace/sheets/api/guides/concepts?hl=es-419)
[![License](https://img.shields.io/badge/License-Private-red)](LICENSE)

## Excel de Datos Diario

🔗 [Google Sheets Link](https://docs.google.com/spreadsheets/d/1wOIIgITBUSB4db2uwnM_JofIMfUnLtAq/edit?gid=1606413032#gid=1606413032)

</div>


---


## 🖥️ Descripción General

Este proyecto está diseñado para **scrapear datos de anuncios de motos** desde diferentes cuentas asociadas a Motick en Wallapop. Además de registrar información básica del anuncio, el sistema analiza diariamente el **histórico de visitas, likes y estado de venta** para construir una visión de la evolución del mercado.

Los datos extraídos se organizan automáticamente en un **Google Sheet compartido**, alimentando un **histórico completo de cambios** que permite identificar tendencias, comparar precios, detectar anuncios vendidos y generar insights útiles para la toma de decisiones comerciales.

---

## 🔧 Funcionalidades

- 🚀 **Extracción automática** de anuncios activos de motos
- 📈 **Análisis histórico** de visitas y likes por anuncio
- ✅ Detección de **anuncios vendidos o eliminados**
- 📂 Registro de datos en una hoja compartida para fácil seguimiento
- 🔍 Preparado para análisis de mercado y visualización de KPIs

---

## 🧠 Tecnologías Usadas

- **Python 3.11**  
  Lenguaje principal del proyecto

- **Selenium (Chrome Headless)**  
  Automatización de la navegación por Wallapop

- **Google Sheets API**  
  Lectura y escritura de datos en hojas de cálculo compartidas

- **OpenPyXL / Pandas**  
  Procesamiento y consolidación de históricos

---

## 📁 Estructura del Repositorio

```
.
├── SCR_DATA_MOTICK.py         # Script principal de scraping
├── ANALISIS_MOTICK.py         # Analizador de histórico y evolución
├── MIGRADOR_EXCELS.py         # Fusión de excels antiguos en histórico
├── historico_motick.xlsx      # Plantilla/archivo con histórico actualizado
└── README.md
```

---

## 📊 Estructura de Datos

| Campo               | Descripción                                       |
|---------------------|---------------------------------------------------|
| Marca               | Marca de la moto                                  |
| Modelo              | Modelo específico                                 |
| Precio              | Precio publicado en el anuncio                    |
| Fecha Publicación   | Fecha original del anuncio                        |
| Estado              | Si sigue activo o ha sido eliminado/vendido       |
| Nº Visitas          | Número acumulado de visitas                       |
| Nº Likes            | Número acumulado de "me gusta"                    |
| URL                 | Enlace directo al anuncio                         |
| Fecha Extracción    | Fecha y hora de la última recolección de datos    |

---

## ⚙️ Configuración Rápida

1. **Clona el repositorio**
   ```bash
   git clone https://github.com/tu-usuario/motick-moto-scraper.git
   cd motick-moto-scraper
   ```

2. **Configura tus credenciales**
   - Añade tu archivo `credentials.json` de la cuenta de servicio de Google
   - Asegúrate de compartir el Google Sheet con el correo de la cuenta de servicio

3. **Ejecuta el scraper**
   ```bash
   python SCR_DATA_MOTICK.py
   ```

4. **Lanza el análisis histórico**
   ```bash
   python ANALISIS_MOTICK.py
   ```

---

###  📞 Contacto
> Para consultas técnicas utilizar sistema **GitHub Issues**

---

## 📄 Licencia

> **Software Propietario** - Desarrollado para operaciones comerciales internas
> Todos los derechos reservados

---

<div align="center">

**Desarrollado por:** Carlos Peraza  
**Versión:** 12.6 • **Fecha:** Agosto 2025

[![motick.com](https://img.shields.io/badge/motick.com-00f1a2?style=for-the-badge&labelColor=2d3748)](https://www.motick.com/)

*Sistema de extracción automatizada para operaciones comerciales*

**© 2025- Todos los derechos reservados**

<<<<<<< HEAD
</div>
=======
</div>
>>>>>>> 8ff232a (Initial commit: Subida del sistema de automatización completa al repo)
