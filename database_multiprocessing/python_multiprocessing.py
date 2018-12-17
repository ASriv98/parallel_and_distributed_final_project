#!/usr/bin/env python3
from multiprocessing import Process, Queue, Manager
from twilio.rest import Client
import time
import random
import MySQLdb
import pandas as pd 
import numpy as np 


 


sentinel = -1
 
def display_graphs():
    pass
 
 
def pack_evaluation(q):

	
	buy_pack = False

	#current cost of packs are 2 USD
	cost_of_pack = 200


	#the probability of choosing specific cards 
	#key is how it is stored in the database, value is the probability of that card showing up in a pack
	probability_by_type = {
	"BlackCommon"	:	0.10556,
	"BlackUncommon"	: 	0.05163,
	"BlackRare"		:	0.01500,
	"BlueCommon"	:	0.10566,
	"BlueUncommon"	:	0.04140,
	"BlueRare"		:	0.01500,
	"RedCommon"		:	0.03167,
	"RedRare"		:	0.00813,
	"GreenCommon"	:	0.10556,
	"GreenUncommon"	:	0.04140,
	"GreenRare"		:	0.01500,
	"ItemCommon"	:	0.09048,
	"ItemUncommon"	:	0.03845,
	"ItemRare"		:	0.01500,
	}


	df = pd.read_csv("12-17-18.csv")
	df2 = df.set_index("Date", drop = False)
	top = df2.head(1)
	#bottom = df2.values[-1].tolist()	
	#print(bottom)
	print(df2)

	while True:
		pack_expected_value = 0

		for card_type in probability_by_type:
			#print(df2[card_type][-1])	
			card_cost = df2[card_type][-1]
			card_probability = probability_by_type[card_type]
			pack_expected_value += card_cost * card_probability
			print("Pack expected value is: ")
			print(pack_expected_value)

			if (pack_expected_value > cost_of_pack):
				buy_pack = True
				break

		if buy_pack == True:
			break

	if (pack_expected_value > cost_of_pack):
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
 	
	 
if __name__ == '__main__':

	q = Queue()
	p = Process(target=pack_evaluation, args=(q,))
	p2 = Process(target=price_alert, args=(q,))
	print("start")
	p.start()
	p2.start()
	p.join()
	p2.join()
	print("ended processes")
