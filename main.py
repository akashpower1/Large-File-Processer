import pandas as pd
import connect
import aggegrate as ag
from config import chunk_size
import queue
import threading
import time
#Connect to Mysql database
db = connect.DB_Connection()
q = queue.Queue()
def worker():
    while True:
        try:
            cnx, error = db.Mysqlconn()
            if error == 0:
                print('Inserting into database.......')
                item = q.get()
                item.to_sql('prod', cnx, if_exists='append', index=False)
                q.task_done()
                cnx.close()

        except Exception as e:
            print(e)


if __name__ == "__main__":
    cnx, error = db.Mysqlconn()
    # If connection is succcessful then move forward
    if error == 0:
        try:
            cnx.execute('Truncate table prod;')
            cnx.close()
            #Creating Worker threads
            for i in range(4):
                t = threading.Thread(target=worker)
                t.daemon = True
                t.start()

            # Read the csv file from working directory and inserting into queue
            for chunk in pd.read_csv('products.csv', chunksize=chunk_size ):
                q.put(chunk)
            q.join()
            print('Insertion completed successfully!!')
            time.sleep(5)

            # aggegration of data according to name
            cnx, error = db.Mysqlconn()
            if error == 0:
                aggegrate_data = ag.aggregated(cnx)
                aggegrate_data.aggregated_prod()
                cnx.close()
        except Exception as e:
            print(e)
            print('Insertion failed!!')


