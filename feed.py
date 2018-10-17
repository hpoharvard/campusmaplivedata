"""
__author__ = "Giovanni Zambotti"
__copyright__ = ""
__credits__ = ["Giovanni Zambotti"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Giovanni Zambotti"
__email__ = "g.zambotti@gmail.com"
__status__ = "Production"
"""

import feedparser, csv, time, datetime
from datetime import date, timedelta
import psycopg2
from psycopg2 import connect

dbname = 'trumba'

# create log file to track all the pdf created and their location
def createTable(info):    
    f.write(info) 

def parseUrl(url):
    d = feedparser.parse(url)
    len(d['entries'])
       
    #d['entries'][4]['title']
    #print (d['entries'][0]['title'])
    #print (d['entries'][len(d['entries']) -1]['title'])

    i = 0
    while i < len(d['entries']):
        #print (d['entries'][i]['description'], i)
        now = datetime.datetime.now()        
        title = (d['entries'][i]['title']).encode('ascii', 'ignore').decode('ascii').replace("'", "")
        location = (d['entries'][i]['description']).encode('ascii', 'ignore').decode('ascii').split(now.strftime("%A")[:3] + '.,')[0].replace('<br', " ").replace('/>', " ").replace("'", "")
        date = time.strftime("%Y%m%d")        
        trumbaDB(title,location,date)
        i += 1

def trumbaDB(title,location,date):    
    sql = "insert into feed(title,location,date) values ('" + title + "','" + location + "', '" + date + "')" 
    conn_string = "host='localhost' dbname='" + dbname + "' user='postgres' password='postgres'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute(sql, (title,location,date))
    # get the number of updated rows
    updated_rows = cur.rowcount
    # Commit the changes to the database
    conn.commit()
    # Close communication with the PostgreSQL database
    cur.close()
    

if __name__ == '__main__':
    filetime = time.strftime("%Y%m%d")
    f = open("feed_" + filetime + '.txt','wb')
    #parseUrl('https://www.trumba.com/calendars/gazette.rss?filterview=Gazette+Classification')
    #parseUrl('https://www.trumba.com/calendars/gazette.rss?events=21')
    parseUrl('https://www.trumba.com/calendars/gazette.rss?date=' + filetime )
    
    f.close()
    
    
