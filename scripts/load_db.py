import os
import logging
from sqlalchemy import create_engine
import yaml
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_log.txt'),
        logging.StreamHandler()
    ]
)

def drop_objects_if_exist(engine):
    """Удаляет существующие функции и view'ы перед созданием новых"""
    try:
        # Функции
        functions_to_drop = ['fn_AmountCategory', 'fn_AgeCategory']
        
        # View'ы
        views_to_drop = ['dm_sessions', 'dm_transactions']
        
        with engine.begin() as conn:
            # Удаляем функции
            for func_name in functions_to_drop:
                drop_sql = f"IF OBJECT_ID('dbo.{func_name}', 'FN') IS NOT NULL DROP FUNCTION dbo.{func_name}"
                try:
                    conn.exec_driver_sql(drop_sql)
                    logging.info(f"Dropped function {func_name} if it existed")
                except Exception as e:
                    logging.warning(f"Could not drop function {func_name}: {str(e)}")
            
            # Удаляем view'ы
            for view_name in views_to_drop:
                drop_sql = f"IF OBJECT_ID('dbo.{view_name}', 'V') IS NOT NULL DROP VIEW dbo.{view_name}"
                try:
                    conn.exec_driver_sql(drop_sql)
                    logging.info(f"Dropped view {view_name} if it existed")
                except Exception as e:
                    logging.warning(f"Could not drop view {view_name}: {str(e)}")
                    
    except Exception as e:
        logging.error(f"Error dropping objects: {str(e)}")
        raise

def check_functions_exist(engine):
    """Проверяет, что функции созданы"""
    try:
        with engine.begin() as conn:
            result = conn.exec_driver_sql("""
                SELECT name 
                FROM sys.objects 
                WHERE type = 'FN' AND name IN ('fn_AmountCategory', 'fn_AgeCategory')
            """)
            functions = [row[0] for row in result]
            logging.info(f"Functions found in database: {functions}")
            return len(functions) == 2
    except Exception as e:
        logging.error(f"Error checking functions: {str(e)}")
        return False

def split_sql_statements(sql_script, file_name):
    """Разделяет SQL скрипт на отдельные батчи"""
    # Для файлов с функциями используем GO как разделитель
    if 'Function' in file_name:
        # Разделяем по GO (case-insensitive, на отдельной строке)
        batches = re.split(r'^\s*GO\s*$', sql_script, flags=re.IGNORECASE | re.MULTILINE)
        result = [batch.strip() for batch in batches if batch.strip()]
        logging.info(f"Split {file_name} into {len(result)} batches using GO delimiter")
        return result
    else:
        # Для остальных файлов используем точку с запятой
        statements = sql_script.split(';')
        result = [stmt.strip() for stmt in statements if stmt.strip()]
        logging.info(f"Split {file_name} into {len(result)} statements using ; delimiter")
        return result

def load_sql_scripts(sql_dir, db_url):
    try:
        engine = create_engine(db_url)
        
        # Удаляем существующие объекты ПЕРЕД выполнением скриптов
        logging.info("Dropping existing functions and views if any...")
        drop_objects_if_exist(engine)
        
        sql_files = [
            'Adding primary keys and indexes.sql',
            'Functions.sql',
            'View dm_sessions.sql',
            'View dm_transactions.sql'
        ]
        
        for sql_file in sql_files:
            file_path = os.path.join(sql_dir, sql_file)
            if not os.path.exists(file_path):
                logging.error(f"SQL file {sql_file} not found")
                raise FileNotFoundError(f"SQL file {sql_file} not found")
            
            logging.info(f"Executing SQL script: {sql_file}")
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Используем новую функцию для разделения
            statements = split_sql_statements(sql_script, sql_file)
            
            executed_count = 0
            with engine.begin() as conn:
                for i, statement in enumerate(statements, 1):
                    statement = statement.strip()
                    if statement:
                        try:
                            logging.debug(f"Executing statement {i}/{len(statements)} from {sql_file}")
                            conn.exec_driver_sql(statement)
                            executed_count += 1
                        except Exception as e:
                            logging.error(f"Error in {sql_file} statement {i}/{len(statements)}: {str(e)}")
                            # Для Functions.sql - прерываем, т.к. view'ы зависят от функций
                            if sql_file == 'Functions.sql':
                                logging.error("Functions.sql failed - stopping execution")
                                raise
                            # Для view'ов - тоже прерываем, т.к. они должны создаться
                            if 'View' in sql_file:
                                logging.error(f"{sql_file} failed - stopping execution")
                                raise
                            continue
            
            logging.info(f"Successfully executed {sql_file} ({executed_count}/{len(statements)} statements)")
            
            # После Functions.sql проверяем, что функции созданы
            if sql_file == 'Functions.sql':
                if check_functions_exist(engine):
                    logging.info("✓ Functions verified successfully")
                else:
                    logging.error("✗ Functions were not created!")
                    raise Exception("Functions were not created properly")
                    
    except Exception as e:
        logging.error(f"Error executing SQL scripts: {str(e)}")
        raise

def main():
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        if not isinstance(config, dict):
            raise ValueError("config.yaml did not load as a dictionary")
        
        sql_dir = os.path.join('scripts', 'sql')
        db_url = config['db_url']
        
        logging.info("Starting SQL scripts execution")
        load_sql_scripts(sql_dir, db_url)
        logging.info("All SQL scripts executed successfully")
    except Exception as e:
        logging.error(f"Failed to execute SQL scripts: {str(e)}")
        raise

if __name__ == "__main__":
    main()