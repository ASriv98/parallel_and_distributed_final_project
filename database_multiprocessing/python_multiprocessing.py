#!/usr/bin/env python3
from multiprocessing import Process, Queue, Manager
from twilio.rest import Client
import time
import random
 


sentinel = -1
 
def display_graphs():
    pass
 
 
def pack_evaluation(q):

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
