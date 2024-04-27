import pandas as pd 
from sqlalchemy import create_engine 
engine = create_engine('postgresql://root:root@thirsty_darwin/ny_texi') 
engine.connect()
#query = """select 1 as col;"""
#pd.read_sql(query, con=engine)
df = pd.read_csv('/var/lib/formula1/data/races.csv')
df.to_sql(name='races_v2', con=engine)
query =  """select * from races_v2 limit 10;"""
df  = pd.read_sql(query, con=engine)
df.head()