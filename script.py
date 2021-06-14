import requests
import xml.etree.ElementTree as ET
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

from parametros import *

def find_son_value(node, tag):
    son = node.find(tag)
    if son is not None:
        return son.text
    return None

data = []
for country in COUNTRIES:

    print(f"Loading {country} info.")
    response = requests.get(f"https://storage.googleapis.com/tarea-4.2021-1.tallerdeintegracion.cl/gho_{country}.xml")
    string_xml = response.content
    tree = ET.fromstring(string_xml)

    for fact in tree:
        gho = fact.find('GHO')
        for indicator_type in INDICATORS:
            if (gho is not None) and (gho.text in INDICATORS[indicator_type]):
                row = [indicator_type]
                for col in COLUMNS:
                    row.append(find_son_value(fact, col))
                data.append(row)

df = pd.DataFrame(data)  # Write in DF and transpose it
df.columns = ["Type"] + COLUMNS  # Update column names

['GHO', 'COUNTRY', 'SEX', 'YEAR', 'GHECAUSES', 'AGEGROUP', 'Display', 'Numeric', 'Low', 'High']

df['YEAR'] = df['YEAR'].apply(lambda x: x if x else None)
df['Numeric'] = df['Numeric'].apply(lambda x: float(x) if x else None)
df['Low'] = df['Low'].apply(lambda x: float(x) if x else None)
df['High'] = df['High'].apply(lambda x: float(x) if x else None)

print(f"Uploading info to GoogleSheets.")

gc = gspread.service_account(filename=AUTH_FILE)
sh = gc.open_by_key(SHEET_ID)
worksheet = sh.get_worksheet(0)
set_with_dataframe(worksheet, df)