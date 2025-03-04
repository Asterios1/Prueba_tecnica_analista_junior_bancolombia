import sqlite3
import pandas as pd
import numpy as np
import logging
import matplotlib.pyplot as plt
from datetime import datetime
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

class DataAnalyzer:
    def __init__(self, db_path):
        """
        Inicializar analizador de datos
        
        Args:
            db_path (str): Ruta a la base de datos SQLite
        """
        self.conn = sqlite3.connect(db_path)
        self.df_api = None
        self.df_commerce = None
        
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
        plt.style.use('default')
        
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
        Load and preprocess data from database
        
        Returns:
            pd.DataFrame: Merged and filtered dataframe
        """
        try:
            # Load tables
            df_api = pd.read_sql("SELECT * FROM apicall", self.conn)
            df_commerce = pd.read_sql("SELECT * FROM commerce", self.conn)
            
            # Convert date column to datetime
            df_api['date_api_call'] = pd.to_datetime(df_api['date_api_call'])
            
            # Filter API calls for July and August 2024
            df_api_filtered = df_api[
                (df_api['date_api_call'].dt.year == 2024) & 
                (df_api['date_api_call'].dt.month.isin([7, 8]))
            ]
            
            # Filter active companies
            df_commerce_active = df_commerce[df_commerce['commerce_status'] == "Active"]
            
            # Merge dataframes
            df_merged = pd.merge(df_api_filtered, df_commerce_active, on="commerce_id", how="inner")
            
            return df_merged
        
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            raise
    
    def calculate_billing(self, df_merged):
        """
        Calculate billing for each company based on specific contract conditions
        
        Args:
            df_merged (pd.DataFrame): Merged dataframe of API calls and commerce data
        
        Returns:
            pd.DataFrame: Billing summary with total charges
        """
        # Group by commerce_id and commerce_name
        grouped = df_merged.groupby(["commerce_id", "commerce_name"]).agg(
            successful=('ask_status', lambda x: (x == "Successful").sum()),
            failed=('ask_status', lambda x: (x != "Successful").sum())
        ).reset_index()
        
        # Apply billing rules
        grouped['total_a_cobrar_sin_iva'] = grouped.apply(self._calculate_company_billing_base, axis=1)
        grouped['total_a_cobrar_con_iva'] = grouped.apply(self._calculate_company_billing, axis=1)
        
        return grouped
    
    def _calculate_company_billing_base(self, row):
        """
        Calculate base billing without IVA or discounts
        """
        return self._calculate_company_billing(row, apply_discount=False, apply_iva=False)
    
    def _calculate_company_billing(self, row, apply_discount=True, apply_iva=True):
        """
        Calculate billing for individual company with specific contract rules
        
        Args:
            row (pd.Series): Row containing company details
            apply_discount (bool): Flag to apply discounts
            apply_iva (bool): Flag to apply IVA
        
        Returns:
            float: Total billing amount
        """
        base = 0  # Base amount without IVA and discounts
        company_name = row['commerce_name'].strip()
        successful_calls = row['successful']
        failed_calls = row['failed']
        
        # Billing rules for each company
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
            
            # Optional 5% discount for excessive failed calls
            if apply_discount and failed_calls > 6000:
                base *= 0.95
        
        elif company_name == "FusionWave Enterprises":
            base = successful_calls * 300
            
            # Optional discounts for failed calls
            if apply_discount:
                if 2500 <= failed_calls <= 4500:
                    base *= 0.95
                elif failed_calls > 4500:
                    base *= 0.92
        
        # Calculate total with or without IVA
        if apply_iva:
            total = base * (1 + self.iva_rate)
        else:
            total = base
        
        return total
    
    def run_billing_process(self, export_path=None):
        """
        Execute the complete billing process
        
        Args:
            export_path (str, optional): Path to export Excel file
        
        Returns:
            pd.DataFrame: Billing summary
        """
        try:
            # Load and process data
            df_merged = self.load_data()
            
            # Calculate billing
            billing_summary = self.calculate_billing(df_merged)
            
            # Log and display results
            logging.info("\nBilling Summary:")
            print(billing_summary)
            
            # Export to Excel if path provided
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
            # Always close the database connection
            self.conn.close()

def main():
    # Ruta de la base de datos SQLite
    DB_PATH = "Datos/database.sqlite"
    
    # Generar nombres de archivo con fecha actual
    fecha_actual = datetime.now().strftime("%Y-%m-%d")
    ANALISIS_EXPORT_PATH = f"reportes/analisis_datos_{fecha_actual}.xlsx"
    FACTURACION_EXPORT_PATH = f"reportes/resumen_facturacion_{fecha_actual}.xlsx"
    
    # Inicializar y ejecutar análisis de datos
    analyzer = DataAnalyzer(DB_PATH)
    resultados_analisis = analyzer.perform_exploratory_data_analysis(export_path=ANALISIS_EXPORT_PATH)
    
    # Inicializar y ejecutar cálculo de facturación
    calculator = BillingCalculator(DB_PATH)
    billing_results = calculator.run_billing_process(export_path=FACTURACION_EXPORT_PATH)

if __name__ == "__main__":
    main()