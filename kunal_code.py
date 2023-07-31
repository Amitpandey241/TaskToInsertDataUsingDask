import concurrent.futures

import psycopg2
from dask.distributed import Client,LocalCluster
import pandas as pd
import dask.dataframe as dd
import os



# dd.to_parquet(df=df,
#               path='abfs://CONTAINER/FILE.parquet'
#               storage_options={'account_name': 'ACCOUNT_NAME',
# dd.to_csv()

def insert_into_db(partition):
    # counter += 1
    print()
    date = partition.loc[:, "Date"].values.compute()

    average_price = partition.loc[:, "AveragePrice"].values.compute()
    total_Volume = partition.loc[:, "TotalVolume"].values.compute()
    total_bags = partition.loc[:, "TotalBags"].values.compute()
    small_bags = partition.loc[:, "SmallBags"].values.compute()
    company_name = "ABCD"
    company_id = 123
    argss = (date,average_price,total_Volume,total_bags,small_bags,company_name,company_id)
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

        cur.execute(
            'INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags,company_name,company_id) VALUES (%s, %s,%s,%s,%s)',
            argss)
        conn.commit()
    except Exception as error:
        print(str(error))
    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()



def main():
    cluster = LocalCluster(n_workers=2, threads_per_worker=2, memory_limit="1GB")
    client = Client(cluster)
    # partision of the file
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/files/new-0.csv'), npartitions=5)
    # print(tuple(dfs.loc[:,"Date"].compute()))
    # submitting the task
    counter = 0
    # for i in range(dfs.npartitions):
    #     print(dfs.partitions[i].compute())


    for i in range(dfs.npartitions):
        partition=dfs.partitions[i]


        # argss = (date, average_price, total_Volume, total_bags, small_bags,company_name,company_id)

        results = client.submit(insert_into_db, partition)
        # new = results.
        # print(results.result())
        # print(results.status)
        print(f"this is rsult {results.result()} and {results.status},")
        logs = []
        if results.status != "finished" and results.status != "pending":
            # logs.append(f"{partition} Partions {counter}")
            resubmit = client.submit(insert_into_db, partition)
            # print(f"resubmit Task partition {partition} and counter {counter}")
        # print(f"{partition} Partions {counter}")
        # del argss

    print("finished the task")



if __name__ == "__main__" :

    main()
    exit()