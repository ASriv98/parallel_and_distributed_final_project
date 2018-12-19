#!/usr/bin/env python3
from multiprocessing import Process, Queue, Manager, Pool
from twilio.rest import Client
import time
import random
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
import sqlalchemy
import matplotlib.dates as md
import cv2

# This is another process: graphing
# Function that takes column of dates and column of prices and plots them i guess title is another input xtick distance another input Save bool as input filename as input
# Threads store function call/args with above
# SQL queries to make threads

# sentinel = -1

 
def pack_evaluation(price_alert_q = None, eval_q = None):
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

	common = ["BlackCommon", "BlueCommon", "RedCommon", "GreenCommon"]
	common_uncommon = ["BlackCommon", "BlackUncommon", "BlueCommon", "BlueUncommon", "RedCommon", "RedUncommon", "GreenCommon", "GreenUncommon"]
	common_uncommon_rare = ["BlackCommon", "BlackUncommon", "BlackRare", "BlueCommon", "BlueUncommon", "BlueRare", "RedCommon", "RedUncommon", "RedRare", "GreenCommon", "GreenUncommon", "GreenRare"]

	def init_SQL_engine(username, password):
	    return sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@artifact.ccysakewgsvk.us-east-1.rds.amazonaws.com:5432/CallToArms".format(username, password))

	engine = init_SQL_engine("bbrucee", "parallel")

	database_df = pd.read_sql_table("CalltoArms", engine,\
						coerce_float=True, parse_dates="Date", columns=None, chunksize=None)

	common_values = np.array([])
	common_uncommon_values = np.array([])
	common_uncommon_rare_values = np.array([])

	for index, row in database_df.iterrows():
		common_value = 0
		for card_type in common:
			common_value += row[card_type]
		common_values = np.append(common_values, common_value)

	for index, row in database_df.iterrows():
		common_uncommon_value = 0
		for card_type in common_uncommon:
			common_uncommon_value += row[card_type]
		common_uncommon_values = np.append(common_uncommon_values, common_uncommon_value)

	for index, row in database_df.iterrows():
		common_uncommon_rare_value = 0
		for card_type in common_uncommon_rare:
			common_uncommon_rare_value += row[card_type]
		common_uncommon_rare_values = np.append(common_uncommon_rare_values, common_uncommon_rare)

	print("DONE")
	print(common_uncommon_values)

	expected_values = np.array([])
	date_array = np.array([])
	for index, row in database_df.iterrows():
		pack_expected_value = 0
		for card_type in probability_by_type:

			card_cost = row[card_type]
			card_probability = probability_by_type[card_type]
			pack_expected_value += card_cost * card_probability
		
		expected_values =  np.append(expected_values, pack_expected_value)
		date_array = np.append(date_array, row["Date"])

	eval_q.put(date_array)
	eval_q.put(expected_values)

	while True:
		# Wait one hour before calculating next value
		time.sleep(60*60)

		database_df = pd.read_sql_table("CalltoArms", engine, index_col="Date",\
						coerce_float=True, parse_dates="Date", columns=None, chunksize=None)

		pack_expected_value = 0
		for card_type in probability_by_type:

			card_cost = database_df[card_type][-1]
			card_probability = probability_by_type[card_type]
			pack_expected_value += card_cost * card_probability

		print("Pack EV is {}".format(pack_expected_value))
		
		eval_q.put(pack_expected_value)
		eval_q.put(database_df["Date"])

		# Alerts subscribers to better pack value
		if pack_expected_value > cost_of_pack and exists(price_alert_q):
			price_alert_q.put(1)

def price_alert(q):
	while True:
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
 	
	
def make_graphs(eval_q = None, image_q = None):
	date_array = eval_q.get()
	eval_array = eval_q.get()
	
	plt.xticks( rotation=25 )
	# ax = plt.gca()
	# xfmt = md.DateFormatter('%Y-%m-%d %H:%M:%S')
	# ax.xaxis.set_major_formatter(xfmt)
	plt.plot_date(date_array, eval_array, '-')
	plt.savefig('temp_graph.png')
	graph = cv2.imread('temp_graph.png')
	image_q.put(graph)
	while True:
		# Queue get commands are blocking so this wont be running forever
		date_array = np.append(date_array, eval_q.get())
		eval_array = np.append(eval_array, eval_q.get())

		plt.xticks( rotation=25 )
		plt.plot_date(date_array, eval_array, '-')
		plt.savefig('temp_graph.png')
		graph = cv2.imread('temp_graph.png')
		image_q.put(graph)



if __name__ == '__main__':

	# Queue used to update tell the price alert to send a text
	price_alert_q = Queue()
	# Queue to send data to the website
	image_q = Queue()
	eval_q = Queue()

	p_eval = Process(target=pack_evaluation, args=(price_alert_q, eval_q, ))
	p_alert = Process(target=price_alert, args=(price_alert_q, ))
	p_graph = Process(target=make_graphs, args=(eval_q, image_q, ))

	print("start")

	p_eval.start()
	p_graph.start()

	p_eval.join()
	p_graph.join()


	print("ended processes")