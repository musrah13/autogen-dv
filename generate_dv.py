import pandas as pd
import db_connect as db
import numpy as np

def csv_to_datavault_tables(tbl_type: str, csv_dir: str) -> None:
    # CSV file path
    csv_file = f'{csv_dir}\\{tbl_type}.csv'
    
    if tbl_type in ['SAT', 'LNK', 'SAT_LNK']:
        if tbl_type == 'LNK':
            table_name = 'DV_ENTITYLINKS'
            query_binds = "(?, ?, ?)"
            generate_datavault_tables(csv_file,table_name,query_binds)
            
            proc_name = 'PCDR_LNK_CREATION'
            conn.execute_proc(F"SET NOCOUNT ON; EXEC {proc_name} @muted = 1")
            conn.commit()
            
        else:
            table_name = 'DV_ENTITY'
            query_binds = "(?, ?, ?, ?, ?)"
            generate_datavault_tables(csv_file,table_name,query_binds)
    else:
        print('provide a table type from \'SAT\', \'LNK\', OR \'SAT_LNK\'')
        
    return None

# Generate a dataframe from a given csv file
def csv_to_dataframe(csv_file: str) -> pd.DataFrame:
    df = pd.read_csv(csv_file)
    df = df.replace(np.nan, None)
    
    return df

# Insert records into db from a dataframe
def dataframe_to_db_insert(table_name: str,query_binds: str,df: pd.DataFrame):
    query = f"INSERT INTO {table_name} VALUES {query_binds}"
    params = [tuple(row) for row in df.values]
    conn.executemany_ddl(query, params)
    
    conn.commit()
    return None

# Generate DV tables from a csv    
def generate_datavault_tables(csv_file: str,table_name: str,query_binds: str):
    df = csv_to_dataframe(csv_file)
    dataframe_to_db_insert(table_name,query_binds,df)
    
    return None
    
if __name__ == "__main__":
    # Set up connection to Azure SQL Server database
    conn = db.AzureSQLConnector(servername='TEST', databasename='TEST', username='TEST', password='TEST')
    conn.connect()
    
    # types of datavault tables
    tbl_types = ['SAT', 'LNK', 'SAT_LNK']
    
    # directory of the csv files from which the datavault tables' attributes to be collected
    csv_dir = 'C:\\Users\\MushfiqurRahman\\Desktop\\dv_azure_spec\\definitions'
    
    # iterate through each datavault table type and generate tables with attributes recorded in the csv files
    for t in tbl_types:
        csv_to_datavault_tables(t,csv_dir)
    
    conn.close()