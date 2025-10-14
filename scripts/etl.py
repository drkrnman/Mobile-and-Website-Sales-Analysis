import pandas as pd
import os
import ast
from sqlalchemy import create_engine, types as sat
import logging
import yaml
from tqdm import tqdm  
import traceback  
import gc

# Logging settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_log.txt'),
        logging.StreamHandler()
    ]
)

# Reading configuration
try:
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    logging.info("Config loaded successfully from config.yaml")
except Exception as e:
    logging.error(f"Failed to load config.yaml: {e}")
    raise

# Centralized error handling function
def handle_error(e, context="Unknown", action="raise"):
    error_msg = f"Error in {context}: {str(e)}\nStack trace: {traceback.format_exc()}"
    logging.error(error_msg)
    if action == "raise":
        raise e
    else:
        logging.warning(f"Skipping step due to error in {context}")

# function to upload df to db
def upload_table(dtype_map, df, table_name, batch_size=config.get('batch_size', 10_000)):
    try:
        engine = create_engine(config['db_url'], fast_executemany=True)
        query = f"IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL DROP TABLE dbo.{table_name};"
        
        with engine.begin() as conn:
            conn.exec_driver_sql(query)
            df.to_sql(
                name=table_name,
                con=conn,
                schema='dbo',
                if_exists='fail',
                index=False,
                dtype=dtype_map,
                chunksize=batch_size
            )
        logging.info(f"Table '{table_name}' successfully uploaded ({len(df)} rows).")
        return True
    except Exception as e:
        handle_error(e, context=f"Uploading table {table_name}", action="raise")
        return False

# Main ETL function
def main():
    try:
        logging.info("Starting ETL process")
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
        batch_size = config.get('batch_size', 10000)
        
        # List of stages for the progress bar
        stages = [
            ("transactions", "Processing transactions"),
            ("transactions_prods", "Processing transactions products"),
            ("click_stream", "Processing click stream"),
            ("events_add_to_cart", "Processing add to cart events"),
            ("products", "Processing products"),
            ("customers", "Processing customers"),
            ("sessions", "Processing sessions")
        ]
        
        # progress bar for the stages  
        for stage, description in tqdm(stages, desc="ETL Stages", unit="stage"):
            logging.info(f"Starting stage: {description}")
            
            if stage == "transactions":
                # 1. Transactions
                try:
                    transactions_path = os.path.join(config['data_dir'], 'transactions.csv')
                    transactions = pd.read_csv(transactions_path)
                    transactions_1 = transactions.copy()
                    transactions_1['created_at'] = pd.to_datetime(transactions_1['created_at']).dt.floor('s')
                    transactions_1['shipment_date_limit'] = pd.to_datetime(transactions_1['shipment_date_limit']).dt.floor('s')
                    transactions_1['has_free_shipping'] = (transactions_1['shipment_fee'] == 0).astype(int)
                    transactions_1['has_promo'] = (transactions_1['promo_amount'] > 0).astype(int)
                    
                    transactions_dtype_map = {
                        'booking_id':            sat.NVARCHAR(None),  
                        'session_id':            sat.NVARCHAR(None),
                        'customer_id':           sat.BigInteger(),
                        'created_at':            sat.DateTime(),
                        'shipment_date_limit':   sat.DateTime(),
                        'days_to_shipment':      sat.Integer(),
                        'promo_flag':            sat.Integer(),
                        'promo_amount':          sat.Numeric(18, 2),
                        'promo_code':            sat.NVARCHAR(None),
                        'payment_method':        sat.NVARCHAR(None),
                        'payment_status':        sat.NVARCHAR(None),
                        'shipment_fee':          sat.Numeric(18, 2),
                        'total_amount':          sat.Numeric(18, 2),
                    }
                    
                    upload_table(transactions_dtype_map, transactions_1, table_name=config['tables']['transactions'])
                except Exception as e:
                    handle_error(e, context="Processing transactions")
            
            elif stage == "transactions_prods":
                # 2. Transactions products
                try:
                    def parse_and_explode_products(df):
                        def safe_parse(x):
                            if isinstance(x, str):
                                return ast.literal_eval(x)
                            return x if isinstance(x, list) else []
                        
                        df = df.copy()
                        df['products'] = df['product_metadata'].apply(safe_parse)
                        exploded = df[['booking_id', 'products']].explode('products')
                        result = pd.DataFrame({
                            'booking_id': exploded['booking_id'],
                            'product_id': exploded['products'].apply(lambda x: x.get('product_id') if isinstance(x, dict) else None),
                            'quantity': exploded['products'].apply(lambda x: x.get('quantity') if isinstance(x, dict) else None),
                            'item_price': exploded['products'].apply(lambda x: x.get('item_price') if isinstance(x, dict) else None)
                        })
                        result['product_amount'] = result['quantity'] * result['item_price']
                        return result
                    
                    transactions_prods_1 = parse_and_explode_products(transactions_1)
                    
                    transactions_prods_dtype_map = {
                        'booking_id':     sat.NVARCHAR(36),
                        'product_id':     sat.BigInteger(),
                        'quantity':       sat.BigInteger(),
                        'item_price':     sat.BigInteger(),
                        'product_amount': sat.BigInteger(),
                    }
                    upload_table(transactions_prods_dtype_map, transactions_prods_1, table_name=config['tables']['transactions_prods'], batch_size=batch_size)
                    logging.info(f"Uploaded {len(transactions_prods_1)} rows to rd_transactions_prods")
                    del transactions_prods_1
                    gc.collect()
                except Exception as e:
                    handle_error(e, context="Processing transactions products")
            
            elif stage == "click_stream":
                # 3. Click stream
                try:
                    click_stream_path = os.path.join(config['data_dir'], 'click_stream.csv')
                    click_stream = pd.read_csv(click_stream_path)
                    click_stream_1 = click_stream.copy()
                    click_stream_1['event_time'] = pd.to_datetime(click_stream_1['event_time']).dt.floor("s")
                except Exception as e:
                    handle_error(e, context="Processing click stream")
            
            elif stage == "events_add_to_cart":
                # 4. Events add to cart
                try:
                    def make_event_table(df, event_name):
                        df_event = df[df['event_name'] == event_name].copy()
                        df_event['event_metadata'] = df_event['event_metadata'].apply(ast.literal_eval)
                        metadata_df = pd.DataFrame(df_event['event_metadata'].tolist(), index=df_event.index)
                        df_event = df_event.drop(columns=['event_metadata', 'event_name']).join(metadata_df)
                        return df_event
                    
                    events_add_to_cart = make_event_table(click_stream_1, 'ADD_TO_CART')
                    events_add_to_cart['prod_amount'] = events_add_to_cart['quantity'] * events_add_to_cart['item_price']
                    events_add_to_cart['event_time'] = events_add_to_cart['event_time'].dt.tz_convert(None)
                    events_add_to_cart['prod_id'] = events_add_to_cart['product_id'].astype('int64')
                    events_add_to_cart['quantity'] = events_add_to_cart['quantity'].astype('int64')
                    events_add_to_cart['item_price'] = events_add_to_cart['item_price'].astype('float64').round(2)
                    
                    events_add_to_cart_1 = events_add_to_cart[[
                        'event_id', 'event_time', 'session_id', 'traffic_source',
                        'product_id', 'quantity', 'item_price', 'prod_amount'
                    ]].copy()
                    events_add_to_cart_1.rename(columns={'product_id': 'prod_id'}, inplace=True)
                    
                    add_to_cart_dtype_map = {   
                        'event_id':       sat.NVARCHAR(None),
                        'event_time':     sat.DateTime(),
                        'session_id':     sat.NVARCHAR(None),
                        'traffic_source': sat.NVARCHAR(31),
                        'prod_id':        sat.BigInteger(),
                        'quantity':       sat.Integer(),
                        'item_price':     sat.Numeric(10, 2),
                        'prod_amount':    sat.Numeric(10, 2)
                    }
                    upload_table(add_to_cart_dtype_map, events_add_to_cart_1, table_name=config['tables']['events_add_to_cart'])
                except Exception as e:
                    handle_error(e, context="Processing add to cart events")
            
            elif stage == "products":
                # 5. Products
                try:
                    product_path = os.path.join(config['data_dir'], 'product.csv')
                    with open(product_path, encoding='utf-8', errors='ignore') as f:
                        lines = ['\t'.join(line.split(',')[:10]).replace('\n',' ').replace('\r',' ') for line in f]
                    
                    products = pd.read_csv(pd.io.common.StringIO('\n'.join(lines)), sep='\t', quotechar='"')
                    products_1 = products.copy()
                    products_1['year'] = products_1['year'].astype('Int64')
                    products_1 = products_1.rename(columns={'id': 'product_id'})
                    products_1['prod_id'] = products_1['product_id']
                    
                    categories_path = config['categories_file']  
                    product_categories_renamed = pd.read_excel(categories_path)

                    products_1['categories_concat'] = (
                        products_1['masterCategory'].astype(str) + "-" +
                        products_1['subCategory'].astype(str) + "-" +
                        products_1['articleType'].astype(str)
                    )
                    
                    products_1 = products_1.merge(
                        product_categories_renamed[['original_name_concat', 'masterCategory_new', 'subCategory_new', 'articleType_new']],
                        left_on='categories_concat',
                        right_on='original_name_concat',
                        how='left'
                    )
                    
                    products_1.columns = products_1.columns.str.strip()
                    rd_products = products_1[[
                        'prod_id', 'productDisplayName', 'masterCategory_new', 'subCategory_new',
                        'articleType_new', 'gender', 'baseColour', 'season', 'year', 'usage'
                    ]].copy()
                    rd_products.columns = [
                        'prod_id', 'prod_name', 'category_level_1', 'category_level_2',
                        'category_level_3', 'gender', 'baseColour', 'season', 'year', 'usage'
                    ]
                    
                    prods_dtype_map = {
                        'prod_id':          sat.BigInteger(),
                        'prod_name':        sat.NVARCHAR(None),
                        'category_level_1': sat.NVARCHAR(255),
                        'category_level_2': sat.NVARCHAR(255),
                        'category_level_3': sat.NVARCHAR(255),
                        'gender':           sat.NVARCHAR(255),
                        'baseColour':       sat.NVARCHAR(255),
                        'season':           sat.NVARCHAR(255),
                        'year':             sat.BigInteger(),
                        'usage':            sat.NVARCHAR(255)
                    }
                    upload_table(prods_dtype_map, rd_products, table_name=config['tables']['products'])
                except Exception as e:
                    handle_error(e, context="Processing products")
            
            elif stage == "customers":
                # 6. Customers
                try:
                    customers_path = os.path.join(config['data_dir'], 'customer.csv')
                    customers = pd.read_csv(customers_path)
                    customers_1 = customers[[
                        'customer_id', 'gender', 'birthdate', 'device_type', 'device_version', 'home_location'
                    ]].copy()
                    customers_1['birthdate'] = pd.to_datetime(customers_1['birthdate'])
                    
                    customers_dtype_map = {
                        'customer_id':     sat.BigInteger(),
                        'gender':          sat.NVARCHAR(255),
                        'birthdate':       sat.DateTime(),
                        'device_type':     sat.NVARCHAR(255),
                        'device_version':  sat.NVARCHAR(255),
                        'home_location':   sat.NVARCHAR(255)
                    }
                    upload_table(customers_dtype_map, customers_1, table_name=config['tables']['customers'])
                except Exception as e:
                    handle_error(e, context="Processing customers")
            
            elif stage == "sessions":
                # 7. Sessions
                try:
                    event_types = [
                        'ADD_PROMO', 'ADD_TO_CART', 'BOOKING', 'CLICK',
                        'HOMEPAGE', 'ITEM_DETAIL', 'PROMO_PAGE', 'SCROLL', 'SEARCH'
                    ]
                    
                    step1 = click_stream_1.copy()
                    for e in event_types:
                        step1[f'{e}_time'] = step1.loc[step1['event_name'] == e, 'event_time']
                    
                    agg_dict = {}
                    for e in event_types:
                        if e in ['ADD_PROMO', 'BOOKING']:
                            agg_dict[f'{e}_time'] = (f'{e}_time', 'min')
                        else:
                            agg_dict[f'{e}_cnt'] = (f'{e}_time', 'count')
                            agg_dict[f'{e}_first_time'] = (f'{e}_time', 'min')
                            agg_dict[f'{e}_last_time'] = (f'{e}_time', 'max')
                    
                    sessions = (
                        step1.groupby(['session_id', 'traffic_source'], as_index=False)
                             .agg(**agg_dict)
                    )
                    
                    for c in sessions.filter(like='_time').columns:
                        sessions[c] = sessions[c].dt.tz_convert(None)
                    
                    base = (
                        click_stream_1.groupby('session_id', as_index=True)
                                      .agg(
                                          session_events_cnt=('event_id', 'count'),
                                          session_start_time=('event_time', 'min'),
                                          session_end_time=('event_time', 'max'),
                                      )
                    )
                    
                    step2_events = ['CLICK', 'SCROLL', 'SEARCH', 'ITEM_DETAIL', 'PROMO_PAGE']
                    step3_events = ['ADD_TO_CART', 'ADD_PROMO']
                    
                    step2 = (
                        click_stream_1[click_stream_1['event_name'].isin(step2_events)]
                        .groupby('session_id')['event_time']
                        .min()
                        .rename('step2_time')
                    )
                    
                    step3 = (
                        click_stream_1[click_stream_1['event_name'].isin(step3_events)]
                        .groupby('session_id')['event_time']
                        .min()
                        .rename('step3_time')
                    )
                    
                    sessions_agg = base.join([step2, step3]).reset_index()
                    
                    for c in sessions_agg.filter(like='_time').columns:
                        sessions_agg[c] = sessions_agg[c].dt.tz_convert(None)
                    
                    sessions = (
                        sessions.merge(sessions_agg, on='session_id', how='left')
                                .drop(columns=['index'], errors='ignore')
                    )
                    
                    sessions_dtype_map = {
                        'session_id':              sat.NVARCHAR(None),
                        'traffic_source':          sat.NVARCHAR(31),
                        'ADD_PROMO_time':          sat.DateTime(),
                        'ADD_TO_CART_cnt':         sat.Integer,     
                        'ADD_TO_CART_first_time':  sat.DateTime(),
                        'ADD_TO_CART_last_time':   sat.DateTime(),
                        'BOOKING_time':            sat.DateTime(),
                        'CLICK_cnt':               sat.Integer,
                        'CLICK_first_time':        sat.DateTime(),
                        'CLICK_last_time':         sat.DateTime(),
                        'HOMEPAGE_cnt':            sat.Integer,
                        'HOMEPAGE_first_time':     sat.DateTime(),
                        'HOMEPAGE_last_time':      sat.DateTime(),
                        'ITEM_DETAIL_cnt':         sat.Integer,   
                        'ITEM_DETAIL_first_time':  sat.DateTime(),
                        'ITEM_DETAIL_last_time':   sat.DateTime(),
                        'PROMO_PAGE_cnt':          sat.Integer,   
                        'PROMO_PAGE_first_time':   sat.DateTime(),
                        'PROMO_PAGE_last_time':    sat.DateTime(),
                        'SCROLL_cnt':              sat.Integer,
                        'SCROLL_first_time':       sat.DateTime(),
                        'SCROLL_last_time':        sat.DateTime(),
                        'SEARCH_cnt':              sat.Integer,
                        'SEARCH_first_time':       sat.DateTime(),
                        'SEARCH_last_time':        sat.DateTime(),
                        'session_events_cnt':      sat.Integer,         
                        'session_start_time':      sat.DateTime(),
                        'session_end_time':        sat.DateTime(),
                        'step2_time':              sat.DateTime(),
                        'step3_time':              sat.DateTime()
                    }
                    
                    upload_table(sessions_dtype_map, sessions, table_name=config['tables']['sessions'])
                except Exception as e:
                    handle_error(e, context="Processing sessions")
        
        logging.info("ETL process completed successfully")
    except Exception as e:
        handle_error(e, context="Main ETL process")

if __name__ == "__main__":
    main()