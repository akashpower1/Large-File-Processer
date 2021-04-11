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
        try:
            cnx, error = db.Mysqlconn()
            if error == 0:
                item = q.get()
                print('Inserting into database.......')
                item.to_sql('prod', cnx, if_exists='append', index=False)
                cnx.close()
                q.task_done()

        except Exception as e:
            print(e)


if __name__ == "__main__":
    try:
        cnx, error = db.Mysqlconn()
        if error == 0:
            cnx.execute('Truncate table prod;')
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






