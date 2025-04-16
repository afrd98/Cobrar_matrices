import os
import sys
import logging
import configparser
import pandas as pd
import shutil
from urllib.parse import quote_plus
from datetime import datetime
from sqlalchemy import create_engine, text

SOURCE_EXCEL_SHEET = 'Base'
DESTINATION_SQL_TABLE = 'ETL_COBRARMATRICES'
RESULT_SQL_VIEW = 'vw_ETL_COBRARMATRICES_RESULTADO'
PROCEDURE_NAME = 'SPETL_COBRARMATRICES'
EXCEL_COLUMN_WIDTH = 40
#uhhh
#Application configuration functions.
def find_local_paths():
    if getattr(sys,'frozen',False):
        base = os.path.dirname(sys.executable)
    else:
        base = os.path.dirname(os.path.abspath(__file__)) 
    ruta_logs = os.path.join(base,'log.txt')
    ruta_conf = os.path.join(base,'config.ini')
    ruta_result = os.path.join(base,'Resultado.xlsx')
    return {'log':ruta_logs,'config':ruta_conf, 'result':ruta_result}

def configure_logging(paths):
    logging.basicConfig(filename=paths['log'], level=logging.INFO, format= "%(asctime)s - %(levelname)s - %(message)s")

def configure_parameters(paths):
    config = configparser.ConfigParser()
    config.read(paths['config'])
    #Database parameters
    server = config["Database"]["server"]
    database = config["Database"]["database"]
    username = config["Database"]["username"]
    password = config["Database"]["password"]
    enc_pass = quote_plus(password)
    driver = config["Database"]["driver"]
    schema = config["Database"]["schema"]
    #Log parameters
    log_path = config["Paths"]["historypath"]
    base_path = config["Paths"]["basepath"]
    connection_string = f"mssql+pyodbc://{username}:{enc_pass}@{server}/{database}?driver={driver}&TrustServerCertificate=yes"
    return{"connection_string":connection_string, "schema":schema, "log_path":log_path, "base_path":base_path}

#Operating system functions
def create_folder(params):
    time = datetime.now().strftime(r'%y%m%d.%H%M')
    folder = os.path.join(params["log_path"],time)
    os.mkdir(folder)
    return folder

def move_files(params,path,folder):
    shutil.copy(src=paths["result"],dst=folder)
    shutil.move(src=params["base_path"],dst=folder)

#Database functions
def write_base_to_database(params):
    base = pd.read_excel(params["base_path"],sheet_name=SOURCE_EXCEL_SHEET)
    engine = create_engine(params["connection_string"])
    with engine.connect() as conn:
        base.to_sql(name=DESTINATION_SQL_TABLE,schema=params["schema"],index=False, con=conn, if_exists='append')

def execute_procedure(params):
    engine = create_engine(params["connection_string"])
    with engine.begin() as conn:
        conn.exec_driver_sql(f'EXEC {params["schema"]}.{PROCEDURE_NAME}')
        #conn.commit()

def clear_table(params):
    engine = create_engine(params["connection_string"])
    with engine.connect() as conn:
        conn.execute(text(f'DELETE FROM {DESTINATION_SQL_TABLE}'))
        conn.commit()

def excel_writer(path,data):
    with pd.ExcelWriter(path=path,engine='xlsxwriter',engine_kwargs={"options":{"strings_to_numbers":True}}) as writer:
        data.to_excel(excel_writer=writer,index=False,sheet_name="Base")    
        workbook = writer.book
        worksheet = writer.sheets["Base"]
        cell_format = workbook.add_format({'bg_color':'#4874c4','font_color':'white', 'bold':True})
        for index, value in enumerate(data.columns):
            #Color de celda y letra
            worksheet.write(0,index,value,cell_format)
            #Ancho de la columna
            worksheet.set_column(index,index,EXCEL_COLUMN_WIDTH)

def export_results(params,paths):
    engine = create_engine(params["connection_string"])
    with engine.connect() as conn:
        data = pd.read_sql(sql=f'SELECT * FROM {params["schema"]}.{RESULT_SQL_VIEW}',con = conn)
    excel_writer(paths['result'],data)
    
if __name__ == '__main__':
    paths = find_local_paths()
    configure_logging(paths)
    params = configure_parameters(paths)

    try:
        if os.path.exists(params["base_path"]):
            logging.info("Iniciando...")
            clear_table(params)
            write_base_to_database(params)
            execute_procedure(params)
            export_results(params,paths)
            #Mover al histórico...
            folder = create_folder(params)
            move_files(params,paths,folder)
            logging.info("Proceso OK")
        else:
            df = pd.DataFrame.from_dict({'Resultado':['Sin matrices...']})
            excel_writer(paths['result'],df)
            logging.info("Sin archivos...")
    except Exception as e:
        logging.error(e)

