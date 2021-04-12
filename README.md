# Large-File-Processer
This is the problem statement to extract large file process it using pandas(dataframe) place it into sql table(prod) calculate the aggegrate on name and store it in another table(aggregated_data).

**Prerequisits** 

pip install sqlalchemy
pip install pymysql
pip install pandas
Msql server

**Input**

Provide input in Config.py file:

user = database user,
password = database password,
host = host address,
database = database name,

**Running**

Python3 (pwd)/main.py

**Tables and Schema**

database : products

tables:
prod_temp(name,sku,description)
prod(name,sku,description,rno)
aggregated_data(count,name)

**Points To Achive**
1. Followed concept of oops.
2. consedered about scaling(use dataframes for manulapation of data).
3. All products datails are inserted in a single table.
4. created an aggegerate table on name and no of products

**Scope of Improvement**

can create automate process to load data to our tables(eg. every day a fresh file is loaded to a ftp server or a cloud storage like aws S3 , the data is processed from the file to a staging table in database and then a merge query(upsert) is run to move it to the main table) an data pipeline can be created for a full automation process.

**Contributions**

All Pull requests are welcome.
