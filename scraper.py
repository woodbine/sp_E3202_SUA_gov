# -*- coding: utf-8 -*-
import sys
reload(sys) # Reload does the trick!
sys.setdefaultencoding('UTF8')
import os
import re
import requests
import scraperwiki
import urllib2
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil.parser import parse

# Set up variables
entity_id = "E3202_SUA_gov"
url = "http://www.shropshire.gov.uk/open-data/supplier-payments-over-%C2%A3500/"
errors = 0
# Set up functions
def validateFilename(filename):
    filenameregex = '^[a-zA-Z0-9]+_[a-zA-Z0-9]+_[a-zA-Z0-9]+_[0-9][0-9][0-9][0-9]_[0-9][0-9]$'
    dateregex = '[0-9][0-9][0-9][0-9]_[0-9][0-9]'
    validName = (re.search(filenameregex, filename) != None)
    found = re.search(dateregex, filename)
    if not found:
        return False
    date = found.group(0)
    year, month = int(date[:4]), int(date[5:7])
    now = datetime.now()
    validYear = (2000 <= year <= now.year)
    validMonth = (1 <= month <= 12)
    if all([validName, validYear, validMonth]):
        return True
def validateURL(url):
    try:
        r = requests.get(url, allow_redirects=True, timeout=20)
        count = 1
        while r.status_code == 500 and count < 4:
            print ("Attempt {0} - Status code: {1}. Retrying.".format(count, r.status_code))
            count += 1
            r = requests.get(url, allow_redirects=True, timeout=20)
        sourceFilename = r.headers.get('Content-Disposition')

        if sourceFilename:
            ext = os.path.splitext(sourceFilename)[1].replace('"', '').replace(';', '').replace(' ', '')
        else:
            ext = os.path.splitext(url)[1]
        validURL = r.status_code == 200
        validFiletype = ext in ['.csv', '.xls', '.xlsx']
        return validURL, validFiletype
    except:
        raise
def convert_mth_strings ( mth_string ):

    month_numbers = {'JAN': '01', 'FEB': '02', 'MAR':'03', 'APR':'04', 'MAY':'05', 'JUN':'06', 'JUL':'07', 'AUG':'08', 'SEP':'09','OCT':'10','NOV':'11','DEC':'12' }
    #loop through the months in our dictionary
    for k, v in month_numbers.items():
#then replace the word with the number

        mth_string = mth_string.replace(k, v)
    return mth_string
# pull down the content from the webpage
html = urllib2.urlopen(url)
soup = BeautifulSoup(html)
# find all entries with the required class
block = soup.find('div', attrs = {'class':'navigation'}).find('ul').find('ul').find('ul')
links = block.find_all('a')
for link in links:
     link_csv = 'http://www.shropshire.gov.uk' +link['href']
     html_csv = urllib2.urlopen(link_csv)
     soup_csv = BeautifulSoup(html_csv)
     block_csv = soup_csv.find('div', attrs = {'class':'content'}).find('ul', 'attachments')
     url_csvs = block_csv.find_all('a')
     for url_csv in url_csvs:
         if '.csv' in url_csv['href']:
             url = 'http://www.shropshire.gov.uk' + url_csv['href']
             csvfiles = url_csv.text
             csvYr = csvfiles.split('Payments ')[-1].split(' ')[0]
             csvMth = csvfiles.split('Payments ')[-1].split(' ')[1]
            # print csvMth
             if '20' not in csvYr:
                 csvYr = csvfiles.split('Payments ')[-1].split(' ')[1]
                 csvMth = csvfiles.split('Payments ')[-1].split(' ')[0][:3]
                 csvMth = convert_mth_strings(csvMth.upper())
             if len(csvMth)<2:
                 csvMth = '0'+csvMth

             filename = entity_id + "_" + csvYr + "_" + csvMth
             file_url = url
             todays_date = str(datetime.now())
             validFilename = validateFilename(filename)
             validURL, validFiletype = validateURL(file_url)
             if not validFilename:
                print filename, "*Error: Invalid filename*"
                print file_url
                errors += 1
                continue
             if not validURL:
                print filename, "*Error: Invalid URL*"
                print file_url
                errors += 1
                continue
             if not validFiletype:
                print filename, "*Error: Invalid filetype*"
                print file_url
                errors += 1
                continue
             scraperwiki.sqlite.save(unique_keys=['l'], data={"l": file_url, "f": filename, "d": todays_date })
             print filename
if errors > 0:
   raise Exception("%d errors occurred during scrape." % errors)