from sqlalchemy import create_engine
import pymysql
import config

class DB_Connection :

    def __init__(self):
        self.user = config.user
        self.password = config.password
        self.host = config.host
        self.database = config.database

    def Mysqlconn(self):
        print('Start connection........')
        try:
            engine = create_engine('mysql+pymysql://'+self.user+':'+self.password+'@'+self.host+'/'+self.database, echo=False)
            cnx = engine.connect()
            print('SQL Connected!!!!')
            return cnx,0
        except Exception as e:
            print(e)
            print('Connection Failed!!')
            return 'err',1




