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
    cur.execute('DROP TABLE IF EXISTS employee')

    create_table = ''' CREATE TABLE IF NOT EXISTS employee(
                            id      int PRIMARY KEY,
                            name    varchar(40) NOT NULL,
                            salary  int,
                            dept_id varchar(30))'''
    cur.execute(create_table)
    insert_script = 'INSERT INTO employee (id, name, salary, dept_id) VALUES (%s, %s,%s,%s)'
    insert_value = [(1,"Amit",12544,'d1'),(2,"Rushi",12544,'d3'),(3,"Pratik",1254544,'d2')]
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
