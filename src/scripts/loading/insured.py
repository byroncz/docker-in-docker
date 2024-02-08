import pandas as pd
from sqlalchemy import create_engine


WORKING_DIRECTORY = '/usr/src/app'
DATABASE_URI = 'postgresql://rfhsduga:oRy8GP3BJ36Gv3Ron_2shajiWK2mRhzO@baasu.db.elephantsql.com/rfhsduga'
ENGINE = create_engine(DATABASE_URI)


if __name__ == '__main__':

    # Leer el archivo .parquet
    df = pd.read_parquet(f'{WORKING_DIRECTORY}/data/cleaned.parquet')

    if not df.empty:

        # Dividir el nombre completo en primer y último nombre si es necesario.
        # Esto asume que 'insured_name' contiene nombres completos que podemos dividir.
        df[['first_name', 'last_name']] = df['insured_name'].str.split(' ', n=1, expand=True)

        # Seleccionar y renombrar las columnas según la estructura de la tabla 'insured'.
        df_insured = df[[
            'first_name', 'last_name', 'insured_gender', 'insured_age', 'insured_address',
            'insured_city', 'insured_state', 'insured_postal_code', 'insured_country', 'policy_number', 'policy_type',
        ]].rename(columns={
            'insured_gender': 'gender',
            'insured_age': 'age',
            'insured_address': 'address',
            'insured_city': 'city',
            'insured_state': 'state',
            'insured_postal_code': 'postal_code',
            'insured_country': 'country'
        })
    
        df_insured.to_sql('insured', con=ENGINE, if_exists='append', index=False)
        print("Datos guardados en la base de datos exitosamente.")

    else:
        print("No hay registros que coincidan con la fecha especificada.")

    ENGINE.dispose()


