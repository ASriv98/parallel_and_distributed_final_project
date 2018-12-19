import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib import style

import sqlalchemy
import mpld3
import _thread
style.use('fivethirtyeight')

def init_SQL_engine(username, password):
    return sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@artifact.ccysakewgsvk.us-east-1.rds.amazonaws.com:5432/CallToArms".format(username, password))


engine = init_SQL_engine("bbrucee", "parallel")



sql_query_test_df = pd.read_sql_table("CalltoArms", engine, index_col="Date",\
					coerce_float=True, parse_dates="Date", columns=None, chunksize=None)

print(sql_query_test_df)
