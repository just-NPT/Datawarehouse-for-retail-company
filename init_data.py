import datetime
import sys

import pandas as pd
import psycopg2
from pymongo import MongoClient
from sqlalchemy import create_engine, text

def recreate_table(connection, table_name):
    drop_table_query = text(f"DROP TABLE IF EXISTS mdm.{table_name[2:].lower()} CASCADE")
    connection.execute(drop_table_query)

    create_table_query = text(open(f'./init_sql/{table_name}.sql','r').read())
    connection.execute(create_table_query)
    connection.commit()

def update_mdm(runday):
    # conn = psycopg2.connect(database="mydb", user="myuser", password="mypassword", host="localhost", port=15432)
    alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@localhost:15432/mydb')
    
    sp_tables = ['D_LOAI_HANG','D_THUONG_HIEU','D_PHAN_LOAI_NGANH_HANG','D_NHOM_SAN_PHAM','D_PACKSIZE','D_DON_VI_TINH','D_DON_VI_DG','D_HT_DONG_GOI','D_NGUON_GOC_SP','D_MO_HINH_MUA','D_TRANG_THAI_SP','D_NCC_SP','D_SAN_PHAM','D_GIA_BAN_VAT']
    kho_tables = ['D_TRANG_THAI_KHO','D_KHU_VUC','D_KHO']

    for table in sp_tables[:]:
        print(f'./data/MDM/{table}/{table}_{runday}')
        connection = alchemyEngine.connect()
        
        # df = pd.read_csv(f'./data/MDM/{table}/{table}_{runday}.csv',index_col=f'MA_{table[2:]}')
        try:
            df = pd.read_csv(f'./data/MDM/{table}/{table}_{runday}.csv',dtype=str)
            df.columns = df.columns.str.lower()
            recreate_table(connection=connection, table_name=table)
            df = df.iloc[::-1].reset_index(drop=True) if table == 'D_NHOM_SAN_PHAM' else df
            frame = df.to_sql(f'{table[2:].lower()}', schema='mdm', con=connection, if_exists='append', index=False)
        except ValueError as vx:
            print(vx)
        except Exception as ex:  
            print(ex)
        else:
            print(f"Table {table} has been recreated successfully.\n\n")

        finally:
            connection.close()
    
    for table in kho_tables[:]:
        print(f'./data/MDM/{table}/{table}_{runday}')
        connection = alchemyEngine.connect()
        df = pd.read_csv(f'./data/MDM/{table}/{table}_{runday}.csv',dtype=str)
        df.columns = df.columns.str.lower()
        df = df.drop_duplicates(subset='ma_kho', keep='last') if table == 'D_KHO' else df
        try:
            recreate_table(connection=connection, table_name=table)

            frame = df.to_sql(f'{table[2:].lower()}', schema='mdm', con=connection, if_exists='append', index=False)
        except ValueError as vx:
            print(vx)
        except Exception as ex:  
            print(ex)
        else:
            print(f"Table {table} has been recreated successfully.\n\n")

        finally:
            connection.close()

def update_v2(runday):
    client = MongoClient('mongodb://admin:password@localhost:27017/')
    db = client.v2_database
    file_path = ''
    df = pd.read_csv(f'./data/v2/invoice/{runday[:4]}/{runday[4:6]}/{runday[6:8]}/invoice.csv')
    item_df = pd.read_csv(f'./data/v2/invoice_items/{runday[:4]}/{runday[4:6]}/{runday[6:8]}/invoice_items.csv')
    grouped_items_df = item_df.groupby('invoice_code').apply(lambda x: x.to_dict(orient='records')).reset_index(name='items')
    df = df.merge(grouped_items_df, how='left', left_on='code', right_on='invoice_code')
    
    df.columns = df.columns.str.lower()
    db['v2_invoice'].delete_many({"created_on": {"$gte": datetime.datetime.strptime(runday, "%Y%m%d") - datetime.timedelta(days=1)}})
    df['created_on'] = pd.to_datetime(df['created_on'], format='%Y-%m-%d %H:%M:%S')
    df.drop('invoice_code', axis=1)
    data_dict = df.to_dict("records")
    # print(df.info())
    db['v2_invoice'].insert_many(data_dict)
    

def update_v1(runday):
    conn = psycopg2.connect(database="mydb", user="myuser", password="mypassword", host="localhost", port=15432)
    alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@localhost:15432/mydb')

    table = ['INVOICE', 'INVOICE_ITEMS']
    connection = alchemyEngine.connect()
    trans = connection.begin()
    delete_query = text("""
                        DELETE FROM v1.invoice_items
                        WHERE invoice_code in (
                                    select code
                                    from v1.invoice
                                    where date(created_on) >= date(:runday));
                        
                        DELETE FROM v1.invoice
                        WHERE date(created_on) >= date(:runday);
                        """)
    connection.execute(delete_query, {'runday': datetime.datetime.strptime(runday, "%Y%m%d") - datetime.timedelta(days=1)})
    trans.commit()
    connection.close()

    for table in table[:]:
        connection = alchemyEngine.connect()

        df = pd.read_csv(f'./data/v1/{table.lower()}/{runday[:4]}/{runday[4:6]}/{runday[6:8]}/{table.lower()}.csv')

        df.columns = df.columns.str.lower()
        # print(df.info())
        try:
            frame = df.to_sql(f'{table.lower()}', schema='v1', con=connection, if_exists='append', index=False)
        except ValueError as vx:
            print(vx)
        except Exception as ex:  
            print(ex)
        else:
            print(f"Table {table} has been recreated successfully.\n\n")

        finally:
            connection.close()

def init():
    alchemyEngine = create_engine('postgresql+psycopg2://myuser:mypassword@localhost:15432/mydb')

    connection = alchemyEngine.connect()
    
    for type in ['mdm','v1','stg','dwh']:
        try:
            trans = connection.begin()
            query = text(open(f'./init_sql/{type}_init.sql','r').read())

            connection.execute(query)
            trans.commit()
        except Exception as e:
            print(type)
            print(e)
            trans.rollback()
    connection.close()

if __name__ == '__main__':
    #get runday
    runday = sys.argv[1]

    if runday == '20240318':
        init()

    update_mdm(runday)

    update_v2(runday)

    update_v1(runday)