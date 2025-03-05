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
    """Configurar logging con rotación de archivos"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Crear directorio de logs si no existe
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
    def __init__(self, db_path):
        """
        Inicializar analizador de datos
        
        Args:
            db_path (str): Ruta a la base de datos SQLite
        """
        try:
            self.conn = sqlite3.connect(db_path)
            self.df_api = None
            self.df_commerce = None
        except sqlite3.Error as e:
            logging.error(f"Error conectando a la base de datos: {e}")
            raise
        
    def __del__(self):
        """Asegurar que la conexión se cierre"""
        if hasattr(self, 'conn'):
            self.conn.close()
            logging.info("Conexión a la base de datos cerrada")
            
    def load_data(self):
        """
        Cargar datos de la base de datos
        
        Returns:
            tuple: DataFrames de API calls y comercios
        """
        try:
            # Cargar tablas
            self.df_api = pd.read_sql("SELECT * FROM apicall", self.conn)
            self.df_commerce = pd.read_sql("SELECT * FROM commerce", self.conn)
            
            # Convertir columna de fecha a datetime
            self.df_api['date_api_call'] = pd.to_datetime(self.df_api['date_api_call'])
            
            return self.df_api, self.df_commerce
        
        except Exception as e:
            logging.error(f"Error cargando datos: {e}")
            raise
    
    def perform_exploratory_data_analysis(self, export_path=None):
        """
        Realizar análisis exploratorio de datos
        
        Args:
            export_path (str, optional): Ruta para exportar resultados
        
        Returns:
            dict: Resultados del análisis
        """
        if self.df_api is None or self.df_commerce is None:
            self.load_data()
        
        # Crear directorio de análisis
        os.makedirs('analisis_resultados', exist_ok=True)
        
        # Resultados del análisis
        analisis_resultados = {}
        
        # 1. Información Básica
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
        
        # 2. Estadísticas Descriptivas
        analisis_resultados['estadisticas_descriptivas'] = {
            'API Calls': self.df_api.describe(include='all').to_dict(),
            'Commerce': self.df_commerce.describe(include='all').to_dict()
        }
        
        # 3. Valores Faltantes
        analisis_resultados['valores_faltantes'] = {
            'API Calls': self.df_api.isnull().sum().to_dict(),
            'Commerce': self.df_commerce.isnull().sum().to_dict()
        }
        
        # 4. Distribución de llamadas por estatus
        status_counts = self.df_api['ask_status'].value_counts()
        analisis_resultados['distribucion_llamadas'] = status_counts.to_dict()
        
        # 5. Distribución de comercios por estado
        commerce_status_counts = self.df_commerce['commerce_status'].value_counts()
        analisis_resultados['distribucion_comercios'] = commerce_status_counts.to_dict()
        
        # 6. Análisis de series temporales
        self.df_api['month'] = self.df_api['date_api_call'].dt.to_period('M')
        monthly_calls = self.df_api.groupby('month')['ask_status'].count()
        analisis_resultados['llamadas_mensuales'] = monthly_calls.to_dict()
        
        # Visualizaciones
        self._create_visualizations()
        
        # Exportar resultados a Excel si se proporciona ruta
        if export_path:
            self._export_analysis_to_excel(analisis_resultados, export_path)
        
        return analisis_resultados
    
    def _create_visualizations(self):
        """
        Crear visualizaciones para el análisis exploratorio
        """
        try:
            plt.style.use('default')
            
            # Crear directorio de resultados si no existe
            os.makedirs('analisis_resultados', exist_ok=True)
            
            # 1. Distribución de llamadas por estado
            plt.figure(figsize=(10, 6))
            self.df_api['ask_status'].value_counts().plot(kind='bar')
            plt.title('Distribución de Llamadas por Estado')
            plt.xlabel('Estado de Llamada')
            plt.ylabel('Número de Llamadas')
            plt.tight_layout()
            plt.savefig('analisis_resultados/distribucion_llamadas.png')
            plt.close()
            
            # 2. Distribución de comercios por estado
            plt.figure(figsize=(10, 6))
            self.df_commerce['commerce_status'].value_counts().plot(kind='pie', autopct='%1.1f%%')
            plt.title('Distribución de Comercios por Estado')
            plt.ylabel('')
            plt.tight_layout()
            plt.savefig('analisis_resultados/distribucion_comercios.png')
            plt.close()
            
            # 3. Llamadas mensuales
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
        Exportar resultados del análisis a Excel
        
        Args:
            analisis_resultados (dict): Resultados del análisis
            export_path (str): Ruta para exportar
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
    def __init__(self, db_path):
        """
        Initialize the billing calculator with database connection
        
        Args:
            db_path (str): Path to the SQLite database
        """
        self.conn = sqlite3.connect(db_path)
        self.iva_rate = 0.19
        
    def load_data(self):
        """
        Cargar y preprocesar datos de la base de datos
        
        Returns:
            pd.DataFrame: Cruzar y filtrar el dataframe
        """
        try:
            # Cargar tablas
            df_api = pd.read_sql("SELECT * FROM apicall", self.conn)
            df_commerce = pd.read_sql("SELECT * FROM commerce", self.conn)
            
            # Convertir columna de fecha a fecha y hora
            df_api['date_api_call'] = pd.to_datetime(df_api['date_api_call'])
            
            # Filtrar llamadas a la API para julio y agosto de 2024
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
        Calcular la facturación de cada empresa en función de las condiciones específicas del contrato
        
        Args:
            df_merged (pd.DataFrame): Resumen de facturación con cargos totales
        
        Returns:
            pd.DataFrame: Billing summary with total charges
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
        Calcular facturación base sin IVA ni descuentos
        """
        return self._calculate_company_billing(row, apply_discount=False, apply_iva=False)
    
    def _calculate_company_billing(self, row, apply_discount=True, apply_iva=True):
        """
        Calcular la facturación de una empresa individual con reglas contractuales específicas
        
        Args:
            row (pd.Series): Fila que contiene los detalles de la empresa
            apply_discount (bool): Marca para aplicar descuentos
            apply_iva (bool): Marca para aplicar IVA
        
        Returns:
            float: Importe total de la facturación
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
    
    def run_billing_process(self, export_path=None):
        """
        Ejecutar el proceso de facturación completo
        
        Args:
            export_path (str, optional): Ruta para exportar archivo Excel
        
        Returns:
            pd.DataFrame: Resumen de facturación
        """
        try:
            # Cargar y procesar datos
            df_merged = self.load_data()
            
            # Calcular facturación
            billing_summary = self.calculate_billing(df_merged)
            
            # Registrar y visualizar resultados
            logging.info("\nBilling Summary:")
            print(billing_summary)
            
            # Exportar a Excel si se proporciona la ruta
            if export_path:
                # Crear directorio si no existe
                os.makedirs(os.path.dirname(export_path), exist_ok=True)
                
                # Formatear columnas numéricas
                billing_summary['total_a_cobrar_sin_iva'] = billing_summary['total_a_cobrar_sin_iva'].round(2)
                billing_summary['total_a_cobrar_con_iva'] = billing_summary['total_a_cobrar_con_iva'].round(2)
                
                # Exportar a Excel
                billing_summary.to_excel(export_path, index=False, 
                                         sheet_name='Resumen Facturación')
                
                logging.info(f"\n--- RESUMEN DE FACTURACIÓN EXPORTADO A: {export_path} ---")
            
            return billing_summary
        
        except Exception as e:
            logging.error(f"Billing process failed: {e}")
        
        finally:
            # Cerrar siempre la conexión a la base de datos
            self.conn.close()

def solicitar_correos():
    """
    Solicitar correos de los destinatarios
    
    Returns:
        list: Lista de correos electrónicos
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
    Enviar correo con múltiples archivos Excel
    
    Args:
        archivos (list): Lista de rutas de archivos Excel
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

def main():
    # Ruta de la base de datos SQLite
    DB_PATH = "Datos/database.sqlite"
    
    # Crear directorio de reportes si no existe
    os.makedirs("reportes", exist_ok=True)
    
    # Generar nombres de archivo con fecha actual
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    ANALISIS_EXPORT_PATH = f"reportes/analisis_datos_{fecha_actual}.xlsx"
    FACTURACION_EXPORT_PATH = f"reportes/resumen_facturacion_{fecha_actual}.xlsx"
    
    try:
        # Inicializar y ejecutar análisis de datos
        analyzer = DataAnalyzer(DB_PATH)
        resultados_analisis = analyzer.perform_exploratory_data_analysis(export_path=ANALISIS_EXPORT_PATH)
        
        # Inicializar y ejecutar cálculo de facturación
        calculator = BillingCalculator(DB_PATH)
        billing_results = calculator.run_billing_process(export_path=FACTURACION_EXPORT_PATH)
        
        # Solicitar correos de destinatarios
        destinatarios = solicitar_correos()
        
        # Configuración de correo
        remitente = ""  # Se solicitará al momento de enviar
        password = ""   # Se solicitará al momento de enviar
        asunto = "Reportes ETL"
        cuerpo = "Adjunto los reportes de análisis y facturación generados por el proceso ETL."
        
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