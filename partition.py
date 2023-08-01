
import psycopg2
from dask.distributed import Client,LocalCluster
import pandas as pd
import dask.dataframe as dd
import os
import concurrent



# dd.to_parquet(df=df,
#               path='abfs://CONTAINER/FILE.parquet'
#               storage_options={'account_name': 'ACCOUNT_NAME',
# dd.to_csv()

def insert_into_db(rows):
    hostname = 'localhost'
    database = 'user'
    username = 'postgres'
    pwd = 'admin'
    port_id = 5432
    conn = None
    cur = None
    try:

        #This commmand establish the connections of postgreys db
        conn = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        )
        #This commands opens the cursor

        cur = conn.cursor()

        cur.executemany(
            'INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags,company_name,company_id) VALUES (%s, %s,%s,%s,%s,%s,%s)',
            rows)
        conn.commit()
    except Exception as error:
        print(str(error))
    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()



def get_rows_for_bulk_insert(partition):
    rows = []
    for index, row in partition.iterrows():
        date = row["Date"]
        average_price = row["AveragePrice"]
        total_Volume = row["TotalVolume"]
        total_bags = row["TotalBags"]
        small_bags = row["SmallBags"]
        company_name = "ABCD"
        company_id = 123
        rows.append((date, average_price, total_Volume, total_bags, small_bags, company_name.isupper(), company_id))
    return rows


def main():
    cluster = LocalCluster(n_workers=2, threads_per_worker=2, memory_limit="1GB")
    client = Client(cluster)
    # partision of the file

    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/files/new-0.csv'), npartitions=5)

    rows_for_bulk_insert = []
    print(dfs.npartitions)
    for i in range(dfs.npartitions):


        partition = dfs.partitions[i]
        rows = client.submit(get_rows_for_bulk_insert, partition)
        rows_for_bulk_insert.extend(rows.result())


    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(insert_into_db, rows_for_bulk_insert)

    print("Finished the task")



if __name__ == "__main__" :

    main()
    exit()