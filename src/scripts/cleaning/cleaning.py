# Importar pandas con manejo de tipos de datos más eficientes
import pandas as pd
import os


WORKING_DIRECTORY = '/usr/src/app'


def clean_data(file_path: str) -> pd.DataFrame:
    """
    Función para limpiar el dataset de una aseguradora.

    Args:
    - file_path: Ruta del archivo CSV a limpiar.

    Returns:
    - DataFrame limpio.
    """
    # Cargar el archivo CSV
    raw_data = pd.read_csv(file_path)

    # Normalización de formatos de fecha
    date_columns = ['policy_start_date', 'policy_end_date', 'claim_date', 'payment_date']
    for col in date_columns:
        raw_data[col] = pd.to_datetime(raw_data[col], errors='coerce')

    # Filtrar el DataFrame para obtener solo las filas donde policy_end_date es igual al día anterior
    yesterday = pd.Timestamp('now').normalize() - pd.Timedelta(days=1)
    data = raw_data[raw_data['policy_end_date'] != yesterday]

    # Imputación de valores nulos para 'gender' y 'insured_gender'
    # Utilizando el método .fillna() directamente en el DataFrame para evitar FutureWarning
    columns_to_fillna = ['gender', 'insured_gender', 'agent_phone', 'insured_city', 'insured_postal_code', 'insured_country']
    for col in columns_to_fillna:
        data.loc[data[col].isnull(), col] = 'Desconocido'
        data[col] = data[col].astype(str)
        # data[col].fillna('Desconocido', inplace=True)

    columns_to_fillna = ['coverage_limit']
    for col in columns_to_fillna:
        data.loc[data[col].isnull(), col] = 0.0
        data[col] = data[col].astype(float)

    return data


if __name__ == '__main__':
    print('Cleaning data...')
    raw_data_path = os.path.join(WORKING_DIRECTORY, 'data', 'MOCK_DATA.csv')
    cleaned_data_path = os.path.join(WORKING_DIRECTORY, 'data', 'cleaned.parquet')

    cleaned_data_df = clean_data(raw_data_path)
    cleaned_data_df.to_parquet(cleaned_data_path)
    
    print(cleaned_data_df.head())
