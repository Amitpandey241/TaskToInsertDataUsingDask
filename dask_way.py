import concurrent.futures
from dotenv import load_dotenv
import psycopg2
from dask.distributed import Client,LocalCluster
import pandas as pd
import dask.dataframe as dd
import os



load_dotenv()
# this function takes an arguments of rows as tuple and inserts in sql table test aslo it makes connections with db
def insert_into_db(argss):
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

        cur.execute(
            'INSERT INTO test (date, average_price, total_Volume, total_bags, small_bags) VALUES (%s, %s,%s,%s,%s)',
            argss)



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


def main():
    # use of cluster
    cluster = LocalCluster(n_workers=2, threads_per_worker=2,memory_limit="1GB")
    client = Client(cluster)
    # partision of the file
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv'), npartitions=5)

    #submitting the task
    # with concurrent.futures.ThreadPoolExecutor() as executer:
    counter = 0
    for partition in range(5):
        for index, row in dfs.partitions[partition].iterrows():
            counter += 1
            argss = (row.Date, row.AveragePrice, row.TotalVolume, row.TotalBags, row.SmallBags)
            results = client.submit(insert_into_db,argss)
            # new = results.
            # print(results.result())
            # print(results.status)
            print(f"this is rsult {results.result()} and {results.status}, {partition} Partions {counter}")
            logs = []
            if results.status != "finished" and results.status != "pending":
                logs.append(f"{partition} Partions {counter}")
                resubmit = client.submit(insert_into_db,argss)
                print(f"resubmit Task partition {partition} and counter {counter}")
            # print(f"{partition} Partions {counter}")
            del argss


    print("finished the task")



if __name__== "__main__":
    main()
