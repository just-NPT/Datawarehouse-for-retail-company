
from datetime import date, datetime, timedelta
import psycopg2
# from pymongo import MongoClient
from psycopg2 import Error
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine,text

def src_to_stg(table_name, version, runday):
    print(f'version: {version}, table: {table_name}')
    src_conn = get_connection(type="source", version=version)
    dwh_conn = get_connection(type="sink")

    df = get_data(conn=src_conn, table_name=table_name, version=version, runday=runday)
    df = transform_df(df, table_name, version, runday)
    insert_data(df=df,table_name=table_name,conn=dwh_conn,version=version, runday=runday)


def transform_df(df, table_name, version, runday):
    if version == 'v2' and table_name == 'INVOICE':
        # df['so_chung_tu'] = df['SoChungTu']
        # df['total_quantity'] = df['totalQuantity']
        # df['total_discount'] = df['totalDiscount']
        # df['total_value'] = df['totalValue']
        # df['sales_order'] = df['SalesOrder']
        df = df[['code', 'status', 'issue_on', 'created_on', 'org', 'total_value', 'total_quantity', 'src_stock_code', 'vendor_id', 'payment_type', 'note_type', 'channel', 'total_discount', 'seq', 'src_ancestors', 'so_chung_tu', 'src_stock']]
    elif version == 'v2' and table_name == 'INVOICE_ITEMS':
        df = df[['discount_rate', 'quantity_change', 'issue_type', 'issue_code', 'price', 'invoice_code', 'product_code', 'discount', 'quantity', 'unit', 'pkg_quantity']]
    # df = df.astype(str)
    df['etl_date'] = runday
    return df

def insert_data(df, table_name, conn, version, runday=None):
    # cursor = conn.cursor()
    # print(delete_query)
    trans = conn.begin()
    try:
        table_name = f'{version}_{table_name}' if version != 'master_data' else f'{table_name}'
        delete_query = text(f"DELETE FROM stg.{table_name.lower()} WHERE etl_date >= DATE('{runday}') or etl_date is null")
        conn.execute((delete_query))

        print(df.shape[0], table_name)
        df.to_sql(table_name.lower(),schema='stg',con=conn,if_exists='append',index=False)
        
        yesterday_str = (runday - timedelta(days=1)).strftime('%Y-%m-%d')
        delete_query = f"DELETE FROM stg.{table_name.lower()} WHERE etl_date < DATE('{yesterday_str}') or etl_date is null"
        conn.execute(text(delete_query))

        trans.commit()
    except ValueError as vx:
        print(vx)
    except (Exception, Error) as e:
        print(f"Error executing : {e}")
        trans.rollback()
    finally:
        conn.close()
    # print(df.shape[0])
    # columns = ', '.join([f'{col}' if col != 'etl_date' else f'{col}' for col in df.columns])
    # values = ', '.join(['%s'] * len(df.columns))
    # insert_query = f"INSERT INTO stg.{table_name} ({columns.upper()}) VALUES ({values})"
    # for row in df.itertuples(index=False):
    #     try:
    #         # print(row)
    #         row_values = [value if (value not in [ 'NULL', '#VALUE!', 'None']) else None for value in row]
    #         cursor.execute(insert_query, row_values)
    #     except (Exception, Error) as e:
    #         # print(row_values)
    #         print(f"Error executing INSERT query: {e}")
    #         conn.rollback()
    #     else:
    #         conn.commit()



def get_data(conn, table_name, version, runday):
    if version == 'v1':
        trans = conn.begin()
        try:
            get_query = open(f"/opt/airflow/dags/DIM_pull/sql/{version.upper()}_{table_name}.sql","r").read()
            df = pd.read_sql_query(text(get_query), conn, params={"runday": runday, "yesterday": runday - timedelta(days=1)})
            
            print("Get data V1 done !!!")
        except Exception as e:
            print(e)
            trans.rollback()
        return df
    elif version == 'master_data':
        # cursor = conn.cursor()
        trans = conn.begin()
        try:
            get_query = open(f"/opt/airflow/dags/DIM_pull/sql/{table_name}.sql","r").read()
            df = pd.read_sql_query(get_query, conn)
            print(df.head(5))
            # print(df.shape[0])
            # conn.execute(get_query)
            # rows = cursor.fetchall()
            # df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            print(f"Get data for table {table_name} done !!!")
        except Exception as e:
            print(e)
            trans.rollback()
        # cursor.close()
        return df
    elif version == 'v2':
        yesterday = runday - timedelta(days=1)
        query = {
            "$and": [
                {"created_on": {"$gte": yesterday},
                 "created_on": {"$lt": runday}}
            ]
        }
        if table_name[:2] == 'D_':
            query = {}
            table_name = table_name[2:]
        if table_name[:7] == 'INVOICE':
            
            collection = conn[f'v2_{table_name[:7].lower()}']
        print(f'v2_{table_name.lower()}')
        documents = collection.find(query)
        data = list(documents)
        print(len(data))
        if table_name == 'INVOICE':
            df = pd.DataFrame(data)
            print(df.info())
            df = df.drop('_id',axis=1)
        elif table_name == 'INVOICE_ITEMS':
            all_items = []
            for invoice in data:
                items = invoice.get('items', [])
                all_items.extend(items)
            df = pd.DataFrame(all_items)
        return df
    else:
        raise Exception("Wrong input version. It should only v1 or v2 !!!")
    
        return None

def get_connection(version=None, type=None):
    if type == 'source' and version == 'v1':
        alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@postgres_db:5432/mydb')
        return alchemyEngine.connect()
    elif type == 'source' and version == 'master_data':
        alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@postgres_db:5432/mydb')
        return alchemyEngine.connect()
    elif type == 'source' and version == 'v2':
        client = MongoClient('mongodb://admin:password@mongodb:27017/')
        db = client['v2_database']
        return db
    elif type == 'sink':
        alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@postgres_db:5432/mydb')
        return alchemyEngine.connect()
# tables = ['D_KHO','D_INVOICE','D_PHAN_LOAI_NGANH_HANG','D_NHOM_SAN_PHAM','D_PACKSIZE','D_DON_VI_TINH','D_DON_VI_DG','D_HT_DONG_GOI','D_NGUON_GOC_SP','D_MO_HINH_MUA','D_TRANG_THAI_SP','D_NCC_SP','D_SAN_PHAM']
# tables = ['D_KHO','D_TRANG_THAI_KHO','D_KHU_VUC']
# tables = ['INVOICE','INVOICE_ITEMS']
# tables = ['D_GIA_BAN_VAT','D_NHOM_SAN_PHAM','D_SAN_PHAM']
def generate_select_query(tables, conn):
    cur = conn.cursor()
    queries = []
    for table in tables:
        schema = 'v2'
        query = f"""
            SELECT string_agg(quote_ident(column_name), ', ')
            FROM information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = '{table[2:].lower()}';
        """
        
        try:
            cur.execute(query)
            column_names = cur.fetchone()[0]
            
            select_query = f"SELECT {column_names} FROM {schema}.{table[2:].lower()};"
            queries.append(select_query)
        except Exception as e:
            print(f"Error generating query for table {table}: {e}")

        with open(f"./sql/{schema}_{table}.sql","w") as file:
            file.write(select_query)
    
    return queries
# print(generate_select_query(tables=tables, conn=psycopg2.connect(database="mydb", user="myuser", password="mypassword", host="localhost", port=15432)))
    
