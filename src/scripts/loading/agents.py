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

        # Dividir el nombre completo en primer y último nombre si es necesario.
        # Esto asume que 'agent_name' contiene nombres completos que podemos dividir.
        df[['first_name', 'last_name']] = df['agent_name'].str.split(' ', n=1, expand=True)

        # Seleccionar y renombrar las columnas según la estructura de la tabla 'agents'.
        df_agent = df[[
            'first_name', 'last_name', 'agent_email', 'agent_phone', 'policy_number', 'policy_type',
        ]].rename(columns={
            'agent_email': 'email',
            'agent_phone': 'phone',
        })

        df_agent.to_sql('agents', con=ENGINE, if_exists='append', index=False)
        print("Datos guardados en la base de datos exitosamente.")

    else:
        print("No hay registros que coincidan con la fecha especificada.")
        
    ENGINE.dispose()