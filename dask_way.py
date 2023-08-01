import concurrent.futures
from dotenv import load_dotenv
import psycopg2
from dask.distributed import Client,LocalCluster
import pandas as pd
import dask.dataframe as dd
import os



load_dotenv()
# this function takes an arguments of rows as tuple and inserts in sql table test aslo it makes connections with db
def insert_into_db(rows):
    hostname = 'localhost'
    database = 'user'
    username = 'postgres'
    pwd = 'admin'
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




        #This command excecutes and inserts the rows in sql table

        cur.executemany(
            'INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags,company_name,company_id,id) VALUES (%s, %s,%s,%s,%s,%s,%s,%s)',
            rows)



        conn.commit()
    except Exception as error:
        print(str(error))
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


# THis is for selecting all the value from the table
def select_all():
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
        cur.execute('SELECT * FROM test')
        conn.commit()

    except Exception as error:
        print(str(error))
    finally:
        if conn is not None:
            conn.close()
        if cur is not None:
            cur.close()
def insert_in_rows(partions):
    rows=[]
    for index,row in partions.iterrows():
        date = row['Date']
        average_price = row['AveragePrice']
        total_Volume = row["TotalVolume"]
        total_bags = row["TotalBags"]
        small_bags = row["SmallBags"]
        company_name = "ABCD"
        company_id = 123
        id = row['id']

        rows.append((date, average_price,total_Volume,total_bags,small_bags,company_name.isupper(),company_id,int(id)))
    return rows



def main():
    # use of cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=2,memory_limit="1GB")
    client = Client(cluster)
    # partision of the file
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv').drop_duplicates(subset=['id']), npartitions=5)
    # dfs = dfs.drop_duplicates(subset=['id'])
    row_for_bulk_insert = []
    for i in range(dfs.npartitions):
        partition = dfs.partitions[i]
        rows = client.submit(insert_in_rows,partition)
        row_for_bulk_insert.extend(rows.result())
        results = client.submit(insert_into_db,row_for_bulk_insert)
        if results.status != 'finished' and results.status != 'pending':
            print("Resubmiting ",i," partition")
            resubmit = client.submit(insert_into_db,row_for_bulk_insert)
        print("partion number: ",i)

        row_for_bulk_insert.clear()

    print("finished the task")



if __name__== "__main__":
    main()
