import pandas as pd
from sqlalchemy import create_engine
import os


WORKING_DIRECTORY = '/usr/src/app'
REPORT_PATH = os.path.join(WORKING_DIRECTORY, 'data', 'report.csv')

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')

DATABASE_URI = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
ENGINE = create_engine(DATABASE_URI)


# Define tu consulta SQL
SQL_QUERY = """
SELECT 
    p.number AS policy_number,
    p.start_date,
    p.end_date,
    p.type AS policy_type,
    p.insurance_company,
    i.first_name AS insured_first_name,
    i.last_name AS insured_last_name,
    a.first_name AS agent_first_name,
    a.last_name AS agent_last_name,
    a.email AS agent_email,
    a.phone AS agent_phone,
    pr.premium_amount,
    pr.deductible_amount,
    pr.coverage_limit,
    pa.payment_status,
    pa.payment_date,
    pa.payment_amount,
    pa.payment_method,
    c.claim_status,
    c.claim_date,
    c.claim_amount,
    c.claim_description
FROM policy p
LEFT JOIN insured i ON p.number = i.policy_number AND p.type = i.policy_type
LEFT JOIN agents a ON p.number = a.policy_number AND p.type = a.policy_type
LEFT JOIN premium pr ON p.number = pr.policy_number AND p.type = pr.policy_type
LEFT JOIN payments pa ON p.number = pa.policy_number AND p.type = pa.policy_type
LEFT JOIN claims c ON p.number = c.policy_number AND p.type = c.policy_type
WHERE p.end_date > '2023-07-01';
"""

if __name__ == '__main__':
    # Utiliza Pandas para ejecutar la consulta y cargar los resultados directamente en un DataFrame
    df = pd.read_sql(SQL_QUERY, con=ENGINE)

    # Guarda el DataFrame como un archivo CSV
    df.to_csv(REPORT_PATH, index=False)

    print("Los datos han sido guardados exitosamente en report.csv.")
