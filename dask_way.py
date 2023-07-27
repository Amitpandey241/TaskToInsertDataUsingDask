import psycopg2
from dask.distributed import Client
import pandas as pd
import dask.dataframe as dd




# def read_insert():
#     df = pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv',index_col=0)
#     df.columns = df.columns.str.replace(' ', '')
#     return df
def insert_into_db(argss):
    hostname = 'localhost'
    database = 'user'
    username = 'postgres'
    pwd = 'admin'
    port_id = 5432
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        )

        cur = conn.cursor()
        partitions = int(input("Enter number"))
        dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv'), npartitions=partitions)

        # insert_script = 'INSERT INTO TestingDB (Date, AveragePrice, Total Volume, Total Bags, Small Bags) VALUES (%s, %s,%s,%s,%s)'
        for partition in range(partitions):
            for index,row in dfs.partitions[partition].iterrows():

                argss = (row.Date,row.AveragePrice,row.TotalVolume,row.TotalBags,row.SmallBags)
                cur.execute('INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags) VALUES (%s, %s,%s,%s,%s)',argss)
                del argss

        cur.execute('SELECT * FROM test')
        for record in cur.fetchall():
            print(record)

        conn.commit()
    except Exception as error:
        print(str(error))
    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()

def main():
    client = Client(n_workers=2, threads_per_worker=2, memory_limit="1GB")
    partitions = int(input("Enter number"))
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv'), npartitions=partitions)

    test_read_fun = insert_into_db()


if __name__== "__main__":
    main()
