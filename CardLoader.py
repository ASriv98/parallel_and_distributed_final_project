from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup
import pandas as pd
from ast import literal_eval
import re
import threading
import sys
import time
import requests
from collections import defaultdict
from functools import reduce
import sqlalchemy

sys.setrecursionlimit(250000)


def parse_card_page(in_url, result_storage, in_tuple):
    tuple_list = []
    raw_html = requests.get(in_url)
    p = BeautifulSoup(raw_html.content)
    check_for_errors = True
    retry = False
    while check_for_errors:
        for stuff in p.find_all('h3'):
            if stuff.text == "You've made too many requests recently. Please wait and try your request again later." or stuff.text == "An error was encountered while processing your request":
                time.sleep(60)
                retry = True
        for stuff in p.find_all('title'):
            if stuff.text == "Access Denied":
                time.sleep(60)
                retry = True
        if retry:
            raw_html = requests.get(in_url)
            p = BeautifulSoup(raw_html.content)
            retry = False
        else:
            check_for_errors = False

    historic_prices = str()
    for js_script in p.find_all('script'):
        for content in reversed(js_script.contents):
            start = str(content).find("var line1=")
            if start != -1:
                end = str(content).find(";", start)
                historic_prices = str(content)[start:end]
                break
    historic_prices = historic_prices.split("\"],[\"")
    historic_prices[0] = historic_prices[0][13:]
    historic_prices[-1] = historic_prices[-1][:-4]
    for historic_price in historic_prices:
        date = historic_price.split(",")[0][:-5]
        if len(historic_price.split(',')) > 1:
            price = int(float(historic_price.split(",")[1]) * 100)
        else:
            price = 0
        tuple_list.append((date, price))
    result_storage.append(in_tuple + tuple([tuple_list]))


def parse_results_page(in_url, outlist, in_color, in_card_type):
    color_parsed = in_color.split('_')[-1]
    if in_card_type == "category_583950_Card_Type%5B%5D=tag_Hero":
        card_type_parsed = "Hero"
    else:
        card_type_parsed = "Regular"
    listings_json = get(in_url).json()['results']
    for listing in listings_json:
        for entry in listing:
            if entry == "name":
                name = listing["name"]
            if entry == "sell_price":
                sell_price = listing["sell_price"]
            if entry == "asset_description":
                market_hash = listing["asset_description"]["market_hash_name"]
                rarity = listing["asset_description"]["type"]
        outlist.append((color_parsed, card_type_parsed, rarity, name, sell_price, market_hash))


def init_SQL_engine(username, password):
    return sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@artifact.ccysakewgsvk.us-east-1.rds.amazonaws.com:5432/CallToArms".format(username, password))


def initial_load(SQL_username, SQL_password):
    # Initialization
    # First query the full card list search across all three pages
    # For each query parse through and get Card Name, Color, Rarity, Card Type and Hash Name
    # Next for each card, query the URL + Hash Name
    # Parse the HTML bottom portion Javascript to get the timestamp and price list
    # Do additional transformations to get Pandas dataframe
    # to_sql

    color_strings = ["category_583950_Card_Color%5B%5D=tag_Black",
                     "category_583950_Card_Color%5B%5D=tag_Green",
                     "category_583950_Card_Color%5B%5D=tag_Red",
                     "category_583950_Card_Color%5B%5D=tag_Blue"]

    card_types = ["category_583950_Card_Type%5B%5D=tag_Hero",
                  "category_583950_Card_Type%5B%5D=tag_Spell&category_583950_Card_Type%5B%5D=tag_Creep&category_583950_Card_Type%5B%5D=tag_Improvement"]

    jobs = []
    result_list = []
    arg_list = []
    for color in color_strings:
        for card_type in card_types:
            query = "{}&{}".format(color, card_type)
            arg_list.append((
                            'https://steamcommunity.com/market/search/render?appid=583950&{}&norender=1&start={}&count={}'.format(
                                query, 0, 100), color, card_type))

    query = "{}".format("&category_583950_Card_Type%5B%5D=tag_Item")
    arg_list.append((
                    'https://steamcommunity.com/market/search/render?appid=583950&{}&norender=1&start={}&count={}'.format(
                        query, 0, 100), "&category_583950_Card_Type%5B%5D=tag_Item",
                    "&category_583950_Card_Type%5B%5D=tag_Item"))
    for arg in arg_list:
        thread = threading.Thread(target=parse_results_page, args=(arg[0], result_list, arg[1], arg[2]))
        jobs.append(thread)

    # # Start threads
    for j in jobs:
        time.sleep(5)
        j.start()

    # Ensure all of the threads have finished
    for j in jobs:
        j.join()

    jobs = []
    price_history_tuple_list = []
    for result in result_list:
        url = "https://steamcommunity.com/market/listings/583950/{}".format(result[5])
        thread = threading.Thread(target=parse_card_page, args=(url, price_history_tuple_list, result))
        jobs.append(thread)

    # Start threads
    for j in jobs:
        time.sleep(5)
        j.start()

    # Ensure all of the threads have finished
    for j in jobs:
        j.join()

    df = pd.DataFrame(price_history_tuple_list)

    colors = ["Black", "Blue", "Red", "Green", "Item"]
    rarities = ["Common Card", "Uncommon Card", "Rare Card"]
    full_price_history = []
    for color in colors:
        for rarity in rarities:
            color_rarity = "{}{}".format(color, rarity.split(" ")[0])
            time_price_dict = defaultdict(int)
            frame = df.loc[(df[0] == color) & (df[2] == rarity)]
            for idx, row in frame.iterrows():
                if row[6] == [('', 0)]:
                    print("No Data Recorded Error")
                else:
                    for date, card_value in row[6]:
                        time_price_dict[date] += card_value
                        if row[1] == "Regular":
                            time_price_dict[date] += 2 * card_value
            color_rarity_df = pd.DataFrame.from_dict(time_price_dict, columns=[color_rarity], orient="index")
            color_rarity_df['Date'] = pd.to_datetime(color_rarity_df.index)
            full_price_history.append(color_rarity_df)
    # print(full_price_history)

    full_df = reduce(lambda x, y: pd.merge(x, y, on='Date'), full_price_history)
    full_df = full_df.sort_values(by='Date')

    engine = init_SQL_engine(SQL_username, SQL_password)

    datatypes = {'Date': sqlalchemy.DateTime(),
                 'BlueCommon': sqlalchemy.types.INTEGER(),
                 'BlueUncommon': sqlalchemy.types.INTEGER(),
                 'BlueRare': sqlalchemy.types.INTEGER(),
                 'BlackCommon': sqlalchemy.types.INTEGER(),
                 'BlackUncommon': sqlalchemy.types.INTEGER(),
                 'BlackRare': sqlalchemy.types.INTEGER(),
                 'RedCommon': sqlalchemy.types.INTEGER(),
                 'RedUncommon': sqlalchemy.types.INTEGER(),
                 'RedRare': sqlalchemy.types.INTEGER(),
                 'GreenCommon': sqlalchemy.types.INTEGER(),
                 'GreenUncommon': sqlalchemy.types.INTEGER(),
                 'GreenRare': sqlalchemy.types.INTEGER(),
                 'ItemCommon': sqlalchemy.types.INTEGER(),
                 'ItemUncommon': sqlalchemy.types.INTEGER(),
                 'ItemRare': sqlalchemy.types.INTEGER()}

    full_df.to_sql("CalltoArms", engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=20,
                   dtype=datatypes)


def hourly_update(SQL_username, SQL_password):
    # Hourly updates
    # First query the full card list search across all three pages
    # For each query parse through and get Card Name, Color, Rarity, Card Type and Hash Name
    # Parse the HTML bottom portion Javascript to get the timestamp and price list
    # Do additional transformations to get Pandas dataframe
    # to_sql

    color_strings = ["category_583950_Card_Color%5B%5D=tag_Black",
                     "category_583950_Card_Color%5B%5D=tag_Green",
                     "category_583950_Card_Color%5B%5D=tag_Red",
                     "category_583950_Card_Color%5B%5D=tag_Blue"]

    card_types = ["category_583950_Card_Type%5B%5D=tag_Hero",
                  "category_583950_Card_Type%5B%5D=tag_Spell&category_583950_Card_Type%5B%5D=tag_Creep&category_583950_Card_Type%5B%5D=tag_Improvement"]

    jobs = []
    result_list = []
    arg_list = []
    for color in color_strings:
        for card_type in card_types:
            query = "{}&{}".format(color, card_type)
            arg_list.append((
                            'https://steamcommunity.com/market/search/render?appid=583950&{}&norender=1&start={}&count={}'.format(
                                query, 0, 100), color, card_type))

    query = "{}".format("&category_583950_Card_Type%5B%5D=tag_Item")
    arg_list.append((
                    'https://steamcommunity.com/market/search/render?appid=583950&{}&norender=1&start={}&count={}'.format(
                        query, 0, 100), "&category_583950_Card_Type%5B%5D=tag_Item",
                    "&category_583950_Card_Type%5B%5D=tag_Item"))
    for arg in arg_list:
        thread = threading.Thread(target=parse_results_page, args=(arg[0], result_list, arg[1], arg[2]))
        jobs.append(thread)

    # # Start threads
    for j in jobs:
        #     time.sleep(3)
        j.start()

    # Ensure all of the threads have finished
    for j in jobs:
        j.join()

    colors = ["Black", "Blue", "Red", "Green", "Item"]
    rarities = ["Common Card", "Uncommon Card", "Rare Card"]
    value_dict = {
        'Date': result_list[0][0],
        'BlueCommon': 0,
        'BlueUncommon': 0,
        'BlueRare': 0,
        'BlackCommon': 0,
        'BlackUncommon': 0,
        'BlackRare': 0,
        'RedCommon': 0,
        'RedUncommon': 0,
        'RedRare': 0,
        'GreenCommon': 0,
        'GreenUncommon': 0,
        'GreenRare': 0,
        'ItemCommon': 0,
        'ItemUncommon': 0,
        'ItemRare': 0
    }

    for result in result_list:
        color_rarity = "{}{}".format(result[1], result[3].split()[0])
        value_contribution = 0
        if result[2] == 'Regular':
            value_contribution += 3 * result[5]
        if result[2] == 'Hero':
            value_contribution += result[5]
        value_dict[color_rarity] += value_contribution

    value_df = pd.DataFrame(value_dict, index=[0])
    value_df['Date'] = pd.to_datetime(value_df['Date'])
    datatypes = {'Date': sqlalchemy.DateTime(),
                 'BlueCommon': sqlalchemy.types.INTEGER(),
                 'BlueUncommon': sqlalchemy.types.INTEGER(),
                 'BlueRare': sqlalchemy.types.INTEGER(),
                 'BlackCommon': sqlalchemy.types.INTEGER(),
                 'BlackUncommon': sqlalchemy.types.INTEGER(),
                 'BlackRare': sqlalchemy.types.INTEGER(),
                 'RedCommon': sqlalchemy.types.INTEGER(),
                 'RedUncommon': sqlalchemy.types.INTEGER(),
                 'RedRare': sqlalchemy.types.INTEGER(),
                 'GreenCommon': sqlalchemy.types.INTEGER(),
                 'GreenUncommon': sqlalchemy.types.INTEGER(),
                 'GreenRare': sqlalchemy.types.INTEGER(),
                 'ItemCommon': sqlalchemy.types.INTEGER(),
                 'ItemUncommon': sqlalchemy.types.INTEGER(),
                 'ItemRare': sqlalchemy.types.INTEGER()}
    engine = init_SQL_engine(SQL_username, SQL_password)
    value_df.to_sql("CalltoArms", engine, schema=None, if_exists='append', index=False, index_label=None, chunksize=20,
                    dtype=datatypes)


if __name__ == "__main__":
    while True:
        time.sleep(3600)
        hourly_update("","")