import concurrent.futures
from dotenv import load_dotenv
import psycopg2
from dask.distributed import Client,LocalCluster
import pandas as pd
import dask.dataframe as dd
import os



load_dotenv()
# this function takes an arguments of rows as tuple and inserts in sql table test aslo it makes connections with db
def insert_into_db(partition):
    hostname ='localhost'
    database ='user'
    username ='postgres'
    pwd ='admin'
    # hostname = 'localhost'
    # database = os.getenv("POSTGRES_DB")
    # username = os.getenv("POSTGRES_USER")
    # pwd = os.getenv("POSTGRES_PASSWORD")
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

        rows = []
        for index, row in partition.iterrows():
            date = row['Date']
            average_price = row['AveragePrice']
            total_Volume = row["TotalVolume"]
            total_bags = row["TotalBags"]
            small_bags = row["SmallBags"]
            company_name = "ABCD"
            company_id = 123
            id = row['id']

            rows.append((date, average_price, total_Volume, total_bags, small_bags, company_name.isupper(), company_id,
                         int(id)))


        #This command excecutes and inserts the rows in sql table

        cur.executemany(
            'INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags,company_name,company_id,id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s)',
            rows)
        rows.clear()


        conn.commit()
    except Exception as error:
        print(str(error))
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def main():
    # use of cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=2,memory_limit="1GB")
    client = Client(cluster)
    # partision of the file
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv').drop_duplicates(subset=['id']), npartitions=5)
    # dfs = dfs.drop_duplicates(subset=['id'])
    row_for_bulk_insert = []
    futures=[]
    for i in range(dfs.npartitions):
        partition = dfs.partitions[i]
        # print(i)

        rowss = client.submit(insert_into_db,partition)
        # rowss
        futures.append(rowss)
        # print(rowss.result())
        while len(futures)>=2:
            print(i)
            for j in futures:

                if j.status == "finished":
                    futures.remove(j)
                elif j.status == "error":
                    resubmit = client.submit(insert_into_db,partition)




if __name__== "__main__":
    main()
