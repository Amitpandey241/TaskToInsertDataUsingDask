from dask.distributed import Client
import pandas as pd
import dask.dataframe as dd




# dd.to_parquet(df=df,
#               path='abfs://CONTAINER/FILE.parquet'
#               storage_options={'account_name': 'ACCOUNT_NAME',
# dd.to_csv()


def main():
    client = Client(n_workers=2, threads_per_worker=2, memory_limit="1GB")

    d = {'col1': [1, 2, 3, 4], 'col2': [5, 6, 7, 8]}
    dfs = dd.from_pandas(pd.read_csv('/home/amitpandey/Desktop/taskOnPostgreysSQL/avocado.csv'),npartitions=5)
    index_0 = dfs.partitions[0]
    index_1 = dfs.partitions[1]
    # print(index_0.head())
    # print()
    # print(index_1.head())
    # df = dd.from_pandas(pd.DataFrame(data=d), npartitions=2)
    # dfs.to_csv("/home/amitpandey/Desktop/taskOnPostgreysSQL/files/new-*.csv")
    dfss = dfs.compute_current_divisions()
    print(dfss)
    for i in range(5):
        for index,row in dfs.partitions[i].iterrows():
            print(row.SmallBags)
            print("this is differten")
            break



if __name__ == "__main__" :

    main()
    exit()