import psycopg2
hostname = 'localhost'
database = 'user'
username = 'postgres'
pwd ='admin'
port_id = 5432
conn =None
cur=None
try:
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    )

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS TestingDB')

    # create_table = ''' CREATE TABLE IF NOT EXISTS TestingDB(
    #                         id      int PRIMARY KEY,
    #                         name    varchar(40) NOT NULL,
    #                         salary  int,
    #                         dept_id varchar(30))'''
    # cur.execute(create_table)
    insert_script = 'INSERT INTO TestingDB (Date, AveragePrice, Total Volume, Total Bags, Small Bags) VALUES (%s, %s,%s,%s)'
    insert_value = "dfa"
    for record in insert_value:
        cur.execute(insert_script,record)

    cur.execute('SELECT * FROM EMPLOYEE')
    for record in cur.fetchall():
        print(record)

    conn.commit()



except Exception as e:
    print(str(e))
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
