import pandas as pd
import connect
import aggegrate as ag
from config import chunk_size

#Connect to Mysql database
db = connect.DB_Connection()
cnx,error = db.Mysqlconn()

if __name__ == "__main__":
    # If connection is succcessful then move forward
    if error == 0:
        try:
            cnx.execute('Truncate table prod;')

            # Read the csv file from working directory
            for chunk in pd.read_csv('products.csv', chunksize=chunk_size ):
                print('Inserting into database.......')
                chunk.to_sql('prod', cnx, if_exists='append', index=False)
            print('Insertion completed successfully!!')

            # aggegration of data according to name
            aggegrate_data = ag.aggregated(cnx)
            aggegrate_data.aggregated_prod()
        except Exception as e:
            print(e)
            print('Insertion failed!!')
        cnx.close()

