from pandas import DataFrame
class aggregated:
    def __init__(self,cnx):
        self.cnx = cnx
    def aggregated_prod(self):
        print('Starting Aggegration......')
        try:
            data = self.cnx.execute("Select count(*),name from prod group by name").fetchall()
            df = DataFrame(data,columns=['count','name'] )
            df.to_sql('aggregated_data', self.cnx, if_exists='replace', index=False)
            print('Aggegration completed!!')
        except Exception as e:
            print(e)
            print('Aggegration failed!!')
