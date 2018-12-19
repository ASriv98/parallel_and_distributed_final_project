#!/usr/bin/env python3
from multiprocessing import Process, Queue, Manager, Pool
from twilio.rest import Client
import time
import random
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt

# This is another process: graphing
# Function that takes column of dates and column of prices and plots them i guess title is another input xtick distance another input Save bool as input filename as input
# Threads store function call/args with above
# SQL queries to make threads

sentinel = -1
 
def make_graphs(title, column_of_prices, column_of_dates):

	print(column_of_dates)
	print(column_of_prices)	

	plt.scatter(column_of_dates,column_of_prices,  alpha = 0.5)
	plt.title(title)

def read_df():
	df = pd.read_csv("12-17-18.csv")
	return df

	


def sequential_data_scrub():



def parallel_data_scrub(df, func):

	df = read_df();
	num_partitions = 10
	num_cores = 4 

	df_split = np.array_split(df, num_partitions)
	pool = Pool(num_cores)
	df = pd.concat(pool.map(func, df_split))
	pool.close()
	pool.join()

	return df





 
def pack_evaluation(q=None):
	#current cost of packs are 2 USD
	cost_of_pack = 200

	#the probability of choosing specific cards 
	#key is how it is stored in the database, value is the probability of that card showing up in a pack
	probability_by_type = {
	"BlackCommon"	:	0.10556,
	"BlackUncommon"	: 	0.04140,
	"BlackRare"		:	0.00813,
	"BlueCommon"	:	0.10566,
	"BlueUncommon"	:	0.04140,
	"BlueRare"		:	0.01500,
	"RedCommon"		:	0.10556,
	"RedUncommon"	:	0.04140,
	"RedRare"		:	0.00013,
	"GreenCommon"	:	0.10556,
	"GreenUncommon"	:	0.04140,
	"GreenRare"		:	0.00013,
	"ItemCommon"	:	0.09048,
	"ItemUncommon"	:	0.03845,
	"ItemRare"		:	0.00813,
	}


	#storing csv to a dataframe, should be the same thing for using a database from SQL queries
	df = pd.read_csv("12-17-18.csv")
	df2 = df.set_index("Date", drop = False)
	top = df2.head(1)
	#bottom = df2.values[-1].tolist()	
	#print(bottom)
	print(df2)

	pack_expected_value = 0
	for card_type in probability_by_type:
		#print(df2[card_type][-1])	
		card_cost = df2[card_type][-1]
		card_probability = probability_by_type[card_type]
		pack_expected_value += card_cost * card_probability

	print("Pack EV is {}".format(pack_expected_value))
	
	if pack_expected_value > cost_of_pack and exists(q):
		q.put(1)

def price_alert(q):

	while True:
		if (q.get() == 1):
			print("found a possible match")
			break

	print("in price alert")
	account_sid = 'AC911ff4517adb89e91404efcc33869bab'
	auth_token = '450bd8ddafdbf5d90572729356373130'
	client = Client(account_sid, auth_token)

	message = client.messages \
			    .create(
			         body='You should purchase a card.',
			         from_='+13612098328',
			         to='+17324849120'
				     )

	print(message.sid)
	print(sentinel)
 	
	
def temp_csv_scrub():
	df = pd.read_csv("12-17-18.csv")
	return(df['Date'], df['BlueCommon'])

if __name__ == '__main__':

	date_list, blue_common_list = temp_csv_scrub()
	date_list = list(date_list)
	blue_common_list = list(blue_common_list)



	make_graphs("Title", date_list, blue_common_list)


	q = Queue()
	p = Process(target=pack_evaluation, args=(q,))
	p2 = Process(target=price_alert, args=(q,))
	print("start")
	p.start()
	p2.start()
	p.join()
	p2.join()
	print("ended processes")
