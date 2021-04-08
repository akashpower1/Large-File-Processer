import pandas as pd
import connect
import aggegrate as ag

if __name__ == "__main__" :
    # Read the csv file from working directory
    products = pd.read_csv('products.csv')

    # Connect to Mysql database
    db = connect.DB_Connection()
    cnx, error = db.Mysqlconn()

    # If connection is succcessful then move forward
    if error == 0:
        try:
            print('Inserting into database.......')
            products.to_sql('prod', cnx, if_exists='replace', index=False)
            print('Insertion completed successfully!!')

            # aggegration of data according to name
            aggegrate_data = ag.aggregated(cnx)
            aggegrate_data.aggregated_prod(products)
        except Exception as e:
            print(e)
            print('Insertion failed!!')
        cnx.close()
