import requests
import json
import re
from pprint import pprint
import sqlite3
import pandas as pd

#set JSON headers for Zacks 
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0"
}

#function to take a url for a page displaying an etfs holdings, parse the data on the page,
#and insert each etf and its respective allocation % into a database table named after the
#etf
def get_etf_data(url):

    #open session and set headers to Zacks headers
    with requests.Session() as req:
        req.headers.update(headers)

        #loop over list of ETFs to pull data for, open URL for specific ETF,
        #and print message specifying which ETF is currently having its data extracted
        for key in keys:
            r = req.get(url.format(key))
            print(f"Extracting: {r.url}")

            #loop over lines in extracted page HTML, look for line specifying beginning of
            #ETF holding data
            for line in r.text.splitlines():
                if not line.startswith('etf_holdings.formatted_data'):
                    continue

                #skip over the etf_holdings.formatted_data part, load rest of line. Some
                #ETFs on Zack's have a lot of null entries for their holdings which will 
                #generate a traceback when running json.loads. Need a try/except to skip over
                #these ETFS without generating a traceback.
                try:
                    data = json.loads(line[30:-1])

                    #check to see if table exists for ETF; if not, create one
                    results = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % key)
                    if len(results.fetchall()) == 0:
                        print("creating table: " + key)
                        cur.execute('''CREATE TABLE {}(holding_id INTEGER PRIMARY KEY, 
                                       ticker TEXT NOT NULL, 
                                       percent_holding REAL NOT NULL)'''.format(key))

                    #loop over lines in ETF holdings table
                    for holding in data:
                        tckr_sym = None

                        #some lines aren't formatted correctly in Zack's tables and need to
                        #have their ticker symbol pulled differently. If they aren't 
                        #processed correctly, they'll be skipped
                        if str(holding[1])[0] == '<':
                            tckr_sym = re.search(r'etf/([^"]*)', holding[1]).group(1)
                        else:
                            tckr_sym = holding[1]

                        #insert ticker symbol and ETF holding % into sql table
                        if tckr_sym and tckr_sym != 'NA' and holding[3] != 'NA':
                            cur.execute('''INSERT INTO {}(ticker, percent_holding) 
                                           VALUES('{}', {})'''.format(key, str(tckr_sym), float(holding[3])))
                            print("Inserted %s into %s" % (tckr_sym, key))  

                    #commit changes to table and close before moving on to the next ETF
                    conn.commit()
                    break
                except:
                    print("Failed to extract data for " + key)
                


#create list of ETFs to pull data for
data_frame = pd.read_excel('etfs_details_type_fund_flow.xlsx', sheet_name='Overview')
keys = data_frame['Symbol'].tolist()

#open connection to sqlite3 database and create cursor to execute queries
conn = None
try:
    conn = sqlite3.connect("etf_data_db.db")
    print("connection successful\n")
except:
    print("connection unsuccesful\n")

cur = conn.cursor()

#run extraction function
get_etf_data("https://www.zacks.com/funds/etf/{}/holding")