"""
__author__ = "Giovanni Zambotti"
__copyright__ = ""
__credits__ = ["Giovanni Zambotti"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Giovanni Zambotti"
__email__ = "g.zambotti@gmail.com"
__status__ = "Development"
"""

import requests, arcgis, time, os
import pandas as pd

from lxml import etree
from datetime import date, timedelta, datetime
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from arcgis.gis import GIS
from arcgis import features
import smtplib
"""
NOTE: All the five dots ..... required the correct informations
"""
gis = GIS(".....", os.getenv("user_devportal"), os.getenv("passwd_devportal"), verify_cert=False)
date_format = "%Y-%m-%dT%H:%M:%S"
# hosted feature service id
fc_id = '519cccf6a4704e4bb97e2bc7a485b17e'

# delete all records from the hosted feature service 
def deleteRecords():
    event_features = gis.content.get(fc_id).layers[0]
    event_fset = event_features.query() #querying without any conditions returns all the features    
    #print (event.df.head())
    all_feature = [f for f in event_fset if f.attributes['objectid'] > 0]
    if(len(all_feature) != 0):
        for i in range(0, len(all_feature)):    
            all_objid = all_feature[i].get_value('objectid')
            print (i, all_objid)
            delete_result = event_features.edit_features(deletes=str(all_objid))
            delete_result

# add all records to the hosted feature service    
def addRecords(eventsdict):
    event_features = gis.content.get(fc_id).layers[0]
    event_fset = event_features.query() #querying without any conditions returns all the features    
    add_result = event_features.edit_features(adds = [eventsdict])
    add_result

def parseUrl(url):
    req = requests.get(url, stream = True)
    req.raw.decode_content = True  # ensure transfer encoding is honoured
    b = etree.parse(req.raw)
    choices = []
    # get root element 
    root = b.getroot()
    #print (root)
    
    loctype = 1
    vorder = 0
    eventid = 0
    location_tag = ''
    for item in root.findall('./channel/item'):        
        # iterate child elements of item 
        for child in item:
            #print (child.tag, child.attrib.get("name"))            
            # get location
            if (child.tag == 'description'):
                location = child.text.split("<br />")[0]
                eventid = eventid + 1
            if (child.tag == '{urn:ietf:params:xml:ns:xcal}location'):
                #print ("LOCATION: " + child.text.replace('\n',' '))
                #location_tag = child.text.replace('\n',' ')
                # 'Location: ' + str(location_tag) + ' --- Description: ' +
                loctype = child.text
            # get title
            if (child.tag == 'title'):
                title = child.text
                
            # get link
            #if (child.tag == 'link'):
            #    link = '<a href="' + child.text + '">link text</a>'
            # get end date
            if (child.tag == '{http://schemas.trumba.com/rss/x-trumba}localend'):
                edate = datetime.strptime(child.text, date_format)
                #print ("END DATE: " + child.text)
            # get start date
            if (child.tag == '{http://schemas.trumba.com/rss/x-trumba}localstart'):
                sdate = datetime.strptime(child.text, date_format)                
                #print ("START DATE: " + child.text)
            # get description
            if (child.tag == '{urn:ietf:params:xml:ns:xcal}description'):
                desc = str(child.text)
                #print (desc)
            # get image url    
            if (child.tag == '{http://schemas.trumba.com/rss/x-trumba}customfield' and child.attrib.get("name") == "Event image"):
                #print (child.text)
                imgurl = child.text
            # get classification
            if (child.tag == '{http://schemas.trumba.com/rss/x-trumba}customfield' and child.attrib.get("name") == "Gazette Classification"):
                c = child.text
                classtype = c.split(',')[0]
                #print (child.text)

        choices.append([location, title, desc, sdate, edate, imgurl, loctype, vorder, eventid, classtype])    
            
    #print (choices)
    newlist = choices
    #print (newlist)    
    fcEvents = []
    fmsg = 'This is a daily update of the Harvard University events from HPAC. \n We will need to address all the locations not listed as a "correct location". \n\n'
    for c in newlist[:]:        
        newlist = choices
        #print (c)
        add_loc = c[1]
        for index, row in parseaddresses().iterrows():
            #fuzzyList = [str(row[1]) + " " + str(row[4])  + " " +  str(row[5]), str(row[2]) + " " + str(row[4])  + " " +  str(row[5]), str(row[3]) + " " + str(row[4])  + " " +  str(row[5])]
            #fuzzyXY = [row[6], row[7]]
            fuzzyList = [row[1], row[2], row[3], row[4], row[5], row[6], row[7]]
            fuzzyXY = [row[9], row[10]]
            d = process.extract(c[0], fuzzyList, limit=1)

            #print ((fuzzyXY[0],fuzzyXY[1], c[1], c[2], c[3], c[4], c[5], c[6]))
            if (d[0][1] > 95):                
                #print ((fuzzyXY[0],fuzzyXY[1], c[1], c[2], c[3], c[4], c[5], c[6], c[7]))
                fc = addFeatures(fuzzyXY[0],fuzzyXY[1], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9])
                #fc = addFeatures(c[0], c[2], c[3], fuzzyXY[0],fuzzyXY[1])
                fcEvents.append(fc)
                addRecords(fc)
                #print (fc)
                add_loc = "Correct location: " + str(c[0]) + ", Score: " + str(d[0][1])
                break
            else:
                continue    
        
        fmsg = fmsg + add_loc + "  \n\n"               
    
    print (fmsg)
    # send a message
    #fmsg1 = ["\n".join(fmsg.replace(u'\xa0', u' ')) for fmsg in fmsg1]
    #sendemail(fmsg.encode("ascii", errors="ignore"))
    #sendemail(fmsg.replace('\u2014', '').replace('\u201c','').replace('\u201d','').replace('\xe9','').replace('\xe1','').replace('\u2018',''))
    sendemail(fmsg.encode('ascii', 'ignore').decode('ascii'))
    
def sendemail(msg):    
    #mailServer = smtplib.SMTP('smtp.office365.com', 587)
    mailServer = smtplib.SMTP('smtp.gmail.com', 587)
    mailServer.ehlo()
    mailServer.starttls()
    #mailServer.login("giovanni_zambotti@harvard.edu", os.getenv("passwd_email"))
    mailServer.login("hpomap@gmail.com", ".....") # input passwd before to run the script
    #body = """Dear Student, \n Please send your report. \n Thank you for your attention"""
    #recipients = ['giovanni_zambotti@harvard.edu', 'gzambotti@gmail.com', 'parvaneh_kossari@harvard.edu', 'james_nelson@harvard.edu']
    recipients = ['giovanni_zambotti@harvard.edu', 'gzambotti@gmail.com']
    subject = "HPAC feeds weekly update - " + str(time.strftime("%Y%m%d"))
    #msg = MIMEText(body)
    message = 'Subject: {}\n\n{}'.format(subject, msg)
    mailServer.sendmail("giovanni_zambotti@harvard.edu", recipients, message)
    print(" \n Sent!")
    mailServer.quit()            


def parseaddresses():
    # read the second csv set
    addr_table_path = r".....\master.csv"
    addr_table = pd.read_table(addr_table_path,delimiter=",")    
    return addr_table        

def addFeatures(x, y, title, desc, sdate, edate, imgurl, loctype, vorder, eventid, classtype):    
    week_event = {"geometry": {"y": y, "x": x}, "attributes": {"name": title , "description": desc, "date_start": sdate, "date_end": edate, "image_url": imgurl, 'unit_name': loctype, 'vertical_order': vorder, "event_id": eventid, "use_type": classtype}}
    return week_event

if __name__ == '__main__':
    filetime = time.strftime("%Y%m%d")
    #print (filetime)
    deleteRecords()        
    parseUrl('https://www.trumba.com/calendars/gazette.rss?date=' + filetime + '&weeks=1&xcal=1')   
    #parseUrl('https://www.trumba.com/calendars/gazette.rss?date=20191202&weeks=1&xcal=1')   
    ###sendemail("test")
