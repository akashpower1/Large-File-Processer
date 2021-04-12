import pandas as pd
import connect
import aggegrate as ag
from config import chunk_size
import queue
import threading
import time
q = queue.Queue()

#Getting connection object
db = connect.DB_Connection()


def worker():
    while True:
        item = q.get()
        try:
            cnx, error = db.Mysqlconn()
            if error == 0:
                print('Inserting into database.......')
                item.to_sql('prod_temp', cnx, if_exists='append', index=False)
                cnx.close()


        except Exception as e:
            print(e)
        q.task_done()


if __name__ == "__main__":
    try:
        cnx, error = db.Mysqlconn()
        if error == 0:
            cnx.execute('Truncate table prod_temp;')
            cnx.close()
        # Creating Worker threads
        for i in range(4):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()

        # Read the csv file from working directory and inserting into queue
        for chunk in pd.read_csv('products.csv', chunksize=chunk_size):
            q.put(chunk)
        q.join()

        cnx, error = db.Mysqlconn()
        if error == 0:
            cnx.execute("insert into prod  select *, row_number() over(partition by sku order by name)  from prod_temp t on duplicate key update name = t.name, description = t.description;")
        time.sleep(5)

        print('Insertion completed successfully!!')

        # aggegration of data according to name
        cnx, error = db.Mysqlconn()
        if error == 0:
            aggegrate_data = ag.aggregated(cnx)
            aggegrate_data.aggregated_prod()
            cnx.close()
    except Exception as e:
        print(e)
        print('Insertion failed!!')






