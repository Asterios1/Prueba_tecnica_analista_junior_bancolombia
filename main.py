import sqlite3
import pandas as pd
import numpy as np
import logging
from logging.handlers import RotatingFileHandler
import matplotlib.pyplot as plt
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def setup_logging():
    """
    Configura el sistema de logging del programa.
    
    Se crean dos handlers:
      - Un handler que guarda los logs en un archivo con rotación (máximo 1 MB por archivo y 5 respaldos).
      - Un handler que muestra los mensajes de log en la consola.
    
    Esto permite tener registros persistentes y visualización en tiempo real de la ejecución.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Crear directorio para los archivos de log si no existe
    os.makedirs('logs', exist_ok=True)
    
    # Configurar handler de archivo con rotación
    file_handler = RotatingFileHandler(
        'logs/etl_process.log', 
        maxBytes=1_048_576,  # 1 MB
        backupCount=5
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Configurar handler de consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Configurar logging al inicio
setup_logging()

class DataAnalyzer:
    """
    Clase encargada de cargar datos desde una base de datos SQLite y realizar un análisis exploratorio.
    
    Atributos principales:
      - conn: Conexión a la base de datos.
      - df_api: DataFrame con los registros de llamadas a la API.
      - df_commerce: DataFrame con los registros de comercios.
    """
    def __init__(self, db_path):
        """
        Inicializa el objeto DataAnalyzer y establece la conexión a la base de datos SQLite.
        
        Args:
            db_path (str): Ruta al archivo de la base de datos SQLite.
        """
        try:
            self.conn = sqlite3.connect(db_path)
            self.df_api = None
            self.df_commerce = None
        except sqlite3.Error as e:
            logging.error(f"Error conectando a la base de datos: {e}")
            raise
        
    def __del__(self):
        """
        Destructor que se asegura de cerrar la conexión a la base de datos al finalizar el uso del objeto.
        """
        if hasattr(self, 'conn'):
            self.conn.close()
            logging.info("Conexión a la base de datos cerrada")
            
    def load_data(self):
        """
        Carga los datos de las tablas 'apicall' y 'commerce' desde la base de datos.
        
        Se realiza la conversión de la columna 'date_api_call' a tipo datetime.
        
        Returns:
            tuple: (DataFrame de API calls, DataFrame de comercios)
        """
        try:
            # Cargar datos de ambas tablas
            self.df_api = pd.read_sql("SELECT * FROM apicall", self.conn)
            self.df_commerce = pd.read_sql("SELECT * FROM commerce", self.conn)
            
            # Convertir la columna de fecha a formato datetime para facilitar análisis posteriores
            self.df_api['date_api_call'] = pd.to_datetime(self.df_api['date_api_call'])
            
            return self.df_api, self.df_commerce
        
        except Exception as e:
            logging.error(f"Error cargando datos: {e}")
            raise
    
    def perform_exploratory_data_analysis(self, export_path=None):
        """
        Realiza un análisis exploratorio de los datos cargados, generando estadísticas descriptivas,
        detección de valores faltantes, distribuciones y series temporales. Además, crea visualizaciones
        y exporta los resultados a un archivo Excel.
        
        Args:
            export_path (str, optional): Ruta del archivo Excel donde se exportarán los resultados.
        
        Returns:
            dict: Diccionario con los resultados del análisis.
        """
        if self.df_api is None or self.df_commerce is None:
            self.load_data()
        
        # Asegurar que existe el directorio para guardar resultados del análisis
        os.makedirs('analisis_resultados', exist_ok=True)
        
        # Resultados del análisis
        analisis_resultados = {}
        
        # 1. Información básica de los datos
        analisis_resultados['info_basica'] = {
            'API Calls': {
                'Total Registros': len(self.df_api),
                'Columnas': list(self.df_api.columns)
            },
            'Commerce': {
                'Total Registros': len(self.df_commerce),
                'Columnas': list(self.df_commerce.columns)
            }
        }
        
        # 2. Estadísticas descriptivas de cada tabla
        analisis_resultados['estadisticas_descriptivas'] = {
            'API Calls': self.df_api.describe(include='all').to_dict(),
            'Commerce': self.df_commerce.describe(include='all').to_dict()
        }
        
        # 3. Conteo de valores faltantes en cada columna
        analisis_resultados['valores_faltantes'] = {
            'API Calls': self.df_api.isnull().sum().to_dict(),
            'Commerce': self.df_commerce.isnull().sum().to_dict()
        }
        
        # 4. Distribución de llamadas por estado (por ejemplo, Successful, Failed, etc.)
        status_counts = self.df_api['ask_status'].value_counts()
        analisis_resultados['distribucion_llamadas'] = status_counts.to_dict()
        
        # 5. Distribución de comercios según su estado
        commerce_status_counts = self.df_commerce['commerce_status'].value_counts()
        analisis_resultados['distribucion_comercios'] = commerce_status_counts.to_dict()
        
        # 6. Análisis de series temporales: agrupación de llamadas por mes
        self.df_api['month'] = self.df_api['date_api_call'].dt.to_period('M')
        monthly_calls = self.df_api.groupby('month')['ask_status'].count()
        analisis_resultados['llamadas_mensuales'] = monthly_calls.to_dict()
        
        # Generar gráficos y visualizaciones a partir de los datos
        self._create_visualizations()
        
        # Exportar el análisis a Excel si se ha proporcionado una ruta de exportación
        if export_path:
            self._export_analysis_to_excel(analisis_resultados, export_path)
        
        return analisis_resultados
    
    def _create_visualizations(self):
        """
        Genera y guarda gráficos representativos del análisis exploratorio:
          - Gráfico de barras para la distribución de llamadas por estado.
          - Gráfico de pastel para la distribución de comercios por estado.
          - Gráfico de línea para la serie temporal de llamadas mensuales.
        
        Los gráficos se guardan en el directorio 'analisis_resultados'.
        """
        try:
            plt.style.use('default')
            
            # Crear directorio de resultados si no existe
            os.makedirs('analisis_resultados', exist_ok=True)
            
            # 1. Gráfico de distribución de llamadas por estado
            plt.figure(figsize=(10, 6))
            self.df_api['ask_status'].value_counts().plot(kind='bar')
            plt.title('Distribución de Llamadas por Estado')
            plt.xlabel('Estado de Llamada')
            plt.ylabel('Número de Llamadas')
            plt.tight_layout()
            plt.savefig('analisis_resultados/distribucion_llamadas.png')
            plt.close()
            
            # 2. Gráfico de distribución de comercios por estado (pie chart)
            plt.figure(figsize=(10, 6))
            self.df_commerce['commerce_status'].value_counts().plot(kind='pie', autopct='%1.1f%%')
            plt.title('Distribución de Comercios por Estado')
            plt.ylabel('')
            plt.tight_layout()
            plt.savefig('analisis_resultados/distribucion_comercios.png')
            plt.close()
            
            # 3. Gráfico de línea para llamadas mensuales
            plt.figure(figsize=(12, 6))
            monthly_calls = self.df_api.groupby(self.df_api['date_api_call'].dt.to_period('M'))['ask_status'].count()
            monthly_calls.plot(kind='line', marker='o')
            plt.title('Número de Llamadas por Mes')
            plt.xlabel('Mes')
            plt.ylabel('Número de Llamadas')
            plt.tight_layout()
            plt.savefig('analisis_resultados/llamadas_mensuales.png')
            plt.close()
            
            logging.info("\n--- VISUALIZACIONES GUARDADAS EN 'analisis_resultados/' ---")
        except Exception as e:
            logging.error(f"Error creando visualizaciones: {e}")
    
    def _export_analysis_to_excel(self, analisis_resultados, export_path):
        """
        Exporta los resultados del análisis exploratorio a un archivo Excel.
        
        Cada hoja del archivo contiene una parte distinta del análisis:
          - Información básica
          - Estadísticas descriptivas
          - Valores faltantes
          - Distribución de llamadas
          - Distribución de comercios
          - Llamadas mensuales
        
        Args:
            analisis_resultados (dict): Diccionario con los resultados del análisis.
            export_path (str): Ruta donde se guardará el archivo Excel.
        """
        try:
            # Crear un escritor de Excel
            with pd.ExcelWriter(export_path) as writer:
                # Información Básica
                pd.DataFrame.from_dict(analisis_resultados['info_basica']['API Calls'], orient='index', columns=['Valor']).to_excel(writer, sheet_name='Info Básica API')
                pd.DataFrame.from_dict(analisis_resultados['info_basica']['Commerce'], orient='index', columns=['Valor']).to_excel(writer, sheet_name='Info Básica Commerce')
                
                # Estadísticas Descriptivas
                pd.DataFrame.from_dict(analisis_resultados['estadisticas_descriptivas']['API Calls']).to_excel(writer, sheet_name='Estadísticas API')
                pd.DataFrame.from_dict(analisis_resultados['estadisticas_descriptivas']['Commerce']).to_excel(writer, sheet_name='Estadísticas Commerce')
                
                # Valores Faltantes
                pd.DataFrame.from_dict(analisis_resultados['valores_faltantes']['API Calls'], orient='index', columns=['Valores Faltantes']).to_excel(writer, sheet_name='Valores Faltantes API')
                pd.DataFrame.from_dict(analisis_resultados['valores_faltantes']['Commerce'], orient='index', columns=['Valores Faltantes']).to_excel(writer, sheet_name='Valores Faltantes Commerce')
                
                # Distribución de Llamadas
                pd.DataFrame.from_dict(analisis_resultados['distribucion_llamadas'], orient='index', columns=['Número de Llamadas']).to_excel(writer, sheet_name='Distribución Llamadas')
                
                # Distribución de Comercios
                pd.DataFrame.from_dict(analisis_resultados['distribucion_comercios'], orient='index', columns=['Número de Comercios']).to_excel(writer, sheet_name='Distribución Comercios')
                
                # Llamadas Mensuales
                pd.DataFrame.from_dict(analisis_resultados['llamadas_mensuales'], orient='index', columns=['Número de Llamadas']).to_excel(writer, sheet_name='Llamadas Mensuales')
            
            logging.info(f"\n--- ANÁLISIS EXPORTADO A: {export_path} ---")
        
        except Exception as e:
            logging.error(f"Error exportando análisis a Excel: {e}")

class BillingCalculator:
    """
    Clase encargada de calcular la facturación de cada empresa según reglas contractuales específicas.
    
    Se conecta a la base de datos, carga y filtra los datos, y aplica distintos criterios de facturación
    dependiendo de la empresa (por ejemplo, descuentos, escalas de precios, IVA, etc.).
    """
    def __init__(self, db_path):
        """
        Inicializa el objeto BillingCalculator y establece la conexión a la base de datos.
        
        Args:
            db_path (str): Ruta al archivo de la base de datos SQLite.
        """
        self.conn = sqlite3.connect(db_path)
        self.iva_rate = 0.19 # Tasa de IVA a aplicar en el cálculo de facturación
    
    def load_data(self, selected_months=None):
        """
        Carga y preprocesa los datos de las tablas 'apicall' y 'commerce'.
        
        Realiza las siguientes tareas:
          - Convierte la columna de fecha en la tabla de API calls a tipo datetime.
          - Filtra las llamadas del año 2024 y, opcionalmente, por los meses seleccionados.
          - Filtra los comercios que están activos.
          - Cruza ambos DataFrames para obtener la información combinada.
        
        Args:
            selected_months (list, optional): Lista de meses (números 1-12) a analizar.
        
        Returns:
            pd.DataFrame: DataFrame resultante de cruzar y filtrar los datos.
        """
        try:
            # Cargar tablas
            df_api = pd.read_sql("SELECT * FROM apicall", self.conn)
            df_commerce = pd.read_sql("SELECT * FROM commerce", self.conn)
            
            # Convertir columna de fecha a fecha y hora
            df_api['date_api_call'] = pd.to_datetime(df_api['date_api_call'])
            
            # Filtrar llamadas a la API por mes si se proporcionan meses
            if selected_months:
                df_api_filtered = df_api[
                    (df_api['date_api_call'].dt.year == 2024) & 
                    (df_api['date_api_call'].dt.month.isin(selected_months))
                ]
            else:
                # Si no se proporcionan meses, usar julio y agosto por defecto
                df_api_filtered = df_api[
                    (df_api['date_api_call'].dt.year == 2024) & 
                    (df_api['date_api_call'].dt.month.isin([7, 8]))
                ]
            
            # Filtrar empresas activas
            df_commerce_active = df_commerce[df_commerce['commerce_status'] == "Active"]
            
            # Cruzar dataframes
            df_merged = pd.merge(df_api_filtered, df_commerce_active, on="commerce_id", how="inner")
            
            return df_merged
        
        except Exception as e:
            logging.error(f"Error cargando los datos: {e}")
            raise
    
    def calculate_billing(self, df_merged):
        """
        Calcula la facturación de cada empresa a partir de los datos combinados.
        
        Se agrupa la información por comercio y se cuentan las llamadas exitosas y fallidas.
        Luego se aplican las reglas de facturación específicas para cada empresa.
        
        Args:
            df_merged (pd.DataFrame): DataFrame con los datos combinados de API calls y comercios.
        
        Returns:
            pd.DataFrame: Resumen de facturación con cargos totales (sin y con IVA).
        """
        # Agrupar por commerce_id y commerce_name
        grouped = df_merged.groupby(["commerce_id", "commerce_name"]).agg(
            successful=('ask_status', lambda x: (x == "Successful").sum()),
            failed=('ask_status', lambda x: (x != "Successful").sum())
        ).reset_index()
        
        # Aplicar reglas de facturación
        grouped['total_a_cobrar_sin_iva'] = grouped.apply(self._calculate_company_billing_base, axis=1)
        grouped['total_a_cobrar_con_iva'] = grouped.apply(self._calculate_company_billing, axis=1)
        
        return grouped
    
    def _calculate_company_billing_base(self, row):
        """
        Calcula la facturación base sin aplicar descuentos ni IVA.
        
        Invoca el método _calculate_company_billing con flags para omitir descuentos e IVA.
        
        Args:
            row (pd.Series): Fila con los datos de facturación de la empresa.
        
        Returns:
            float: Importe base calculado.
        """
        return self._calculate_company_billing(row, apply_discount=False, apply_iva=False)
    
    def _calculate_company_billing(self, row, apply_discount=True, apply_iva=True):
        """
        Calcula la facturación total de una empresa aplicando reglas contractuales.
        
        Para cada empresa se define una tarifa según el número de llamadas exitosas y se aplican
        descuentos en función de la cantidad de llamadas fallidas. Finalmente, se suma el IVA si se solicita.
        
        Args:
            row (pd.Series): Fila con los datos de facturación (nombre, llamadas exitosas y fallidas).
            apply_discount (bool): Indica si se deben aplicar descuentos.
            apply_iva (bool): Indica si se debe incluir el IVA en el cálculo final.
        
        Returns:
            float: Importe total de la facturación.
        """
        base = 0  # Importe base sin IVA ni descuentos
        company_name = row['commerce_name'].strip()
        successful_calls = row['successful']
        failed_calls = row['failed']
        
        # Normas de facturación para cada empresa
        if company_name == "Innovexa Solutions":
            base = successful_calls * 300
        
        elif company_name == "NexaTech Industries":
            if successful_calls <= 10000:
                rate = 250
            elif successful_calls <= 20000:
                rate = 200
            else:
                rate = 170
            base = successful_calls * rate
        
        elif company_name == "QuantumLeap Inc":
            base = successful_calls * 600
        
        elif company_name == "Zenith Corp":
            rate = 250 if successful_calls <= 22000 else 130
            base = successful_calls * rate
            
            # Descuento opcional del 5% por exceso de llamadas fallidas
            if apply_discount and failed_calls > 6000:
                base *= 0.95
        
        elif company_name == "FusionWave Enterprises":
            base = successful_calls * 300
            
            # Descuentos opcionales por llamadas fallidas
            if apply_discount:
                if 2500 <= failed_calls <= 4500:
                    base *= 0.95
                elif failed_calls > 4500:
                    base *= 0.92
        
        # Calcular total con o sin IVA
        if apply_iva:
            total = base * (1 + self.iva_rate)
        else:
            total = base
        
        return total
    
    def run_billing_process(self, export_path=None, selected_months=None):
        """
        Ejecuta el proceso completo de facturación:
          - Carga y filtra los datos según los meses seleccionados.
          - Calcula la facturación para cada comercio.
          - Añade información adicional (NIT, correo y última fecha de llamada).
          - Exporta el resumen a un archivo Excel si se especifica.
          - Registra el resumen en el log y lo muestra por pantalla.
        
        Args:
            export_path (str, optional): Ruta para exportar el resumen de facturación a Excel.
            selected_months (list, optional): Meses (1-12) a analizar.
        
        Returns:
            pd.DataFrame: DataFrame con el resumen de facturación.
        """
        try:
            # Cargar y procesar datos con los meses seleccionados
            df_merged = self.load_data(selected_months)
            
            # Calcular facturación
            billing_summary = self.calculate_billing(df_merged)
            
            # Obtener fecha más reciente por comercio (para 'date_api_call')
            fechas_por_comercio = df_merged.groupby('commerce_id')['date_api_call'].max()
            
            # Agregar columnas adicionales
            extra_columns = df_merged.groupby('commerce_id')[['commerce_nit', 'commerce_email']].first()
            
            # Unir columnas adicionales al resumen de facturación
            billing_summary = billing_summary.set_index('commerce_id').join(extra_columns).join(fechas_por_comercio).reset_index()
            
            # Convertir los meses numéricos a nombres de mes
            if selected_months:
                meses_nombres = [datetime(2024, mes, 1).strftime('%B') for mes in selected_months]
                meses_str = ', '.join(meses_nombres)
            else:
                meses_nombres = ['Julio', 'Agosto']
                meses_str = 'Julio, Agosto'
            
            # Reemplazar la fecha con los nombres de meses
            billing_summary['date_api_call'] = meses_str
            
            # Renombrar columnas
            billing_summary = billing_summary.rename(columns={
                'date_api_call': 'Fecha', 
                'commerce_name': 'Nombre', 
                'commerce_nit': 'Nit', 
                'total_a_cobrar_sin_iva': 'Valor_comision', 
                'total_a_cobrar_con_iva': 'Valor_Total', 
                'successful': 'Conseguidas', 
                'failed': 'Falladas', 
                'commerce_email': 'Correo'
            })
            
            # Calcular IVA
            billing_summary['Valor_iva'] = billing_summary['Valor_Total'] - billing_summary['Valor_comision']
            
            # Reordenar columnas en el orden especificado
            columnas_ordenadas = [
                'Fecha', 
                'Nombre', 
                'Nit', 
                'Valor_comision', 
                'Valor_iva', 
                'Valor_Total', 
                'Conseguidas', 
                'Falladas', 
                'Correo'
            ]
            billing_summary = billing_summary[columnas_ordenadas]
            
            # Registrar y visualizar resultados
            logging.info("\nBilling Summary:")
            print(billing_summary)
            
            # Exportar a Excel si se proporciona la ruta
            if export_path:
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(export_path), exist_ok=True)
                
                # Formatear columnas numéricas
                columnas_numericas = ['Valor_comision', 'Valor_iva', 'Valor_Total']
                billing_summary[columnas_numericas] = billing_summary[columnas_numericas].round(2)
                
                # Exportar a Excel
                with pd.ExcelWriter(export_path) as writer:
                    # Escribir el resumen de facturación
                    billing_summary.to_excel(writer, index=False, sheet_name='Resumen Facturación')
                    
                    # Agregar una hoja con información de meses analizados
                    info_meses = pd.DataFrame({
                        'Información': ['Meses Analizados'],
                        'Detalle': [meses_str]
                    })
                    info_meses.to_excel(writer, index=False, sheet_name='Información')
                
                logging.info(f"\n--- RESUMEN DE FACTURACIÓN EXPORTADO A: {export_path} ---")
            
            return billing_summary
        
        except Exception as e:
            logging.error(f"Billing process failed: {e}")
        
        finally:
            # Cerrar siempre la conexión a la base de datos
            self.conn.close()

def solicitar_correos():
    """
    Solicita al usuario que ingrese las direcciones de correo electrónico de los destinatarios.
    
    Se realiza una validación básica (presencia de '@' y '.') y se permite la confirmación de la lista ingresada.
    
    Returns:
        list: Lista de correos electrónicos ingresados y confirmados.
    """
    while True:
        try:
            num_destinatarios = int(input("¿Cuántos correos de destinatarios desea ingresar? "))
            if num_destinatarios <= 0:
                print("Debe ingresar al menos un correo.")
                continue
            
            destinatarios = []
            for i in range(num_destinatarios):
                while True:
                    correo = input(f"Ingrese el correo electrónico #{i+1}: ").strip()
                    # Validación básica de correo electrónico
                    if '@' in correo and '.' in correo:
                        destinatarios.append(correo)
                        break
                    else:
                        print("Correo electrónico inválido. Intente de nuevo.")
            
            # Confirmar correos
            print("\nCorreos ingresados:")
            for correo in destinatarios:
                print(correo)
            
            confirmacion = input("\n¿Son correctos estos correos? (s/n): ").lower()
            if confirmacion == 's':
                return destinatarios
        except ValueError:
            print("Entrada inválida. Por favor, ingrese un número.")

def enviar_correo_excel(remitente, password, destinatarios, asunto, cuerpo, archivos):
    """
    Envía un correo electrónico con archivos adjuntos (en este caso, archivos Excel).
    
    Configura una conexión SMTP con Gmail, adjunta cada archivo (verificando su existencia) y maneja posibles errores de autenticación o envío.
    
    Args:
        remitente (str): Dirección de correo del remitente.
        password (str): Contraseña del remitente (se solicita de forma segura si no se proporciona).
        destinatarios (list): Lista de direcciones de correo destinatarias.
        asunto (str): Asunto del correo.
        cuerpo (str): Cuerpo del mensaje en texto plano.
        archivos (list): Lista de rutas a los archivos Excel a adjuntar.
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = remitente
        msg['To'] = ", ".join(destinatarios)
        msg['Subject'] = asunto
        msg.attach(MIMEText(cuerpo, 'plain'))
        
        for archivo in archivos:
            if not os.path.exists(archivo):
                logging.warning(f"Archivo no encontrado: {archivo}")
                continue
            
            with open(archivo, 'rb') as adjunto:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(adjunto.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(archivo)}')
                msg.attach(part)
        
        with smtplib.SMTP('smtp.gmail.com', 587) as servidor:
            servidor.starttls()
            
            # Solicitar credenciales de forma segura
            if not remitente:
                remitente = input("Ingrese su correo electrónico de Gmail: ")
            
            if not password:
                import getpass
                password = getpass.getpass("Ingrese la contraseña de su correo (no se mostrará): ")
            
            servidor.login(remitente, password)
            servidor.sendmail(remitente, destinatarios, msg.as_string())
        
        logging.info("Correo enviado con éxito")
        print("Correo enviado con éxito.")
    except smtplib.SMTPAuthenticationError:
        logging.error("Error de autenticación. Verifique sus credenciales.")
        print("Error de autenticación. Verifique sus credenciales.")
    except smtplib.SMTPException as e:
        logging.error(f"Error de SMTP al enviar correo: {e}")
        print(f"Error de SMTP al enviar correo: {e}")
    except Exception as e:
        logging.error(f"Error inesperado al enviar correo: {e}")
        print(f"Error inesperado al enviar correo: {e}")

def solicitar_meses():
    """
    Solicita al usuario que seleccione los meses que desea analizar.
    
    Muestra una lista numerada de los meses (1-12) con sus nombres y permite al usuario ingresar varios meses separados por comas.
    
    Returns:
        list: Lista de meses (en formato numérico) seleccionados y confirmados por el usuario.
    """
    while True:
        try:
            # Imprimir meses disponibles
            print("\nMeses disponibles:")
            for mes in range(1, 13):
                print(f"{mes}: {datetime(2024, mes, 1).strftime('%B')}")
            
            # Solicitar entrada de meses
            entrada_meses = input("\nIngrese los meses que desea analizar (separados por coma, ejemplo: 7,8): ").strip()
            
            # Convertir entrada a lista de meses
            meses_seleccionados = [int(mes.strip()) for mes in entrada_meses.split(',')]
            
            # Validar meses
            if all(1 <= mes <= 12 for mes in meses_seleccionados):
                # Confirmar selección
                print("\nMeses seleccionados:")
                for mes in meses_seleccionados:
                    print(datetime(2024, mes, 1).strftime('%B'))
                
                confirmacion = input("\n¿Son correctos estos meses? (s/n): ").lower()
                if confirmacion == 's':
                    return meses_seleccionados
            else:
                print("Por favor, ingrese números de mes válidos entre 1 y 12.")
        
        except ValueError:
            print("Entrada inválida. Por favor, ingrese números de mes separados por coma.")

def main():
    """
    Función principal del programa.
    
    Realiza los siguientes pasos:
      1. Define la ruta de la base de datos y crea el directorio para reportes.
      2. Solicita al usuario los meses a analizar y configura los nombres de archivo de salida.
      3. Ejecuta el análisis exploratorio y el cálculo de facturación.
      4. Solicita los correos de los destinatarios.
      5. Envía los reportes generados por correo electrónico.
    """
    # Ruta de la base de datos SQLite
    DB_PATH = "Datos/database.sqlite"
    
    # Crear directorio de reportes si no existe
    os.makedirs("reportes", exist_ok=True)
    
    # Generar nombres de archivo con fecha actual
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    
    try:
        # Solicitar meses para análisis
        meses_seleccionados = solicitar_meses()
        
        # Nombres de archivos basados en meses seleccionados
        meses_str = '_'.join(str(mes) for mes in meses_seleccionados)
        ANALISIS_EXPORT_PATH = f"reportes/analisis_datos_{meses_str}_{fecha_actual}.xlsx"
        FACTURACION_EXPORT_PATH = f"reportes/resumen_facturacion_{meses_str}_{fecha_actual}.xlsx"
        
        # Inicializar y ejecutar análisis de datos
        analyzer = DataAnalyzer(DB_PATH)
        resultados_analisis = analyzer.perform_exploratory_data_analysis(export_path=ANALISIS_EXPORT_PATH)
        
        # Inicializar y ejecutar cálculo de facturación con meses seleccionados
        calculator = BillingCalculator(DB_PATH)
        billing_results = calculator.run_billing_process(
            export_path=FACTURACION_EXPORT_PATH, 
            selected_months=meses_seleccionados
        )
        
        # Solicitar correos de destinatarios
        destinatarios = solicitar_correos()
        
        # Configuración de correo
        remitente = ""  # Se solicitará al momento de enviar
        password = ""   # Se solicitará al momento de enviar
        asunto = f"Reportes ETL - Meses {meses_str}"
        cuerpo = f"Adjunto los reportes de análisis y facturación generados para los meses {meses_str}."
        
        # Enviar reportes por correo
        enviar_correo_excel(
            remitente, 
            password, 
            destinatarios, 
            asunto, 
            cuerpo, 
            [ANALISIS_EXPORT_PATH, FACTURACION_EXPORT_PATH]
        )
    
    except Exception as e:
        logging.error(f"Error en el proceso principal: {e}")
        print(f"Ocurrió un error: {e}")

if __name__ == "__main__":
    main()