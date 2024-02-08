import pandas as pd
from sqlalchemy import create_engine


WORKING_DIRECTORY = '/usr/src/app'
DATABASE_URI = 'postgresql://rfhsduga:oRy8GP3BJ36Gv3Ron_2shajiWK2mRhzO@baasu.db.elephantsql.com/rfhsduga'
ENGINE = create_engine(DATABASE_URI)


if __name__ == '__main__':

    # Leer el archivo .parquet
    df = pd.read_parquet(f'{WORKING_DIRECTORY}/data/cleaned.parquet')

    if not df.empty:

        # Seleccionar y renombrar las columnas según la estructura de la tabla 'claims'.
        df_claims = df[[
            'policy_number', 'claim_status', 'claim_date', 
            'claim_amount', 'claim_description', 'policy_type',    
        ]]

        df_claims.to_sql('claims', con=ENGINE, if_exists='append', index=False)
        print("Datos guardados en la base de datos exitosamente.")

    else:
        print("No hay registros que coincidan con la fecha especificada.")

    ENGINE.dispose()


