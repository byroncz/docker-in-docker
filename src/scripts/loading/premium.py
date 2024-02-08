import pandas as pd
from sqlalchemy import create_engine
import os


WORKING_DIRECTORY = '/usr/src/app'

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')

DATABASE_URI = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
ENGINE = create_engine(DATABASE_URI)


if __name__ == '__main__':

    # Leer el archivo .parquet
    df = pd.read_parquet(f'{WORKING_DIRECTORY}/data/cleaned.parquet')

    if not df.empty:

        # Seleccionar y renombrar las columnas según la estructura de la tabla 'premium'.
        df_premium = df[[
            'premium_amount', 'deductible_amount', 'coverage_limit', 'policy_number', 'policy_type',
        ]]

        df_premium.to_sql('premium', con=ENGINE, if_exists='append', index=False)

        print("Datos guardados en la base de datos exitosamente.")


    else:
        print("No hay registros que coincidan con la fecha especificada.")

    ENGINE.dispose()


