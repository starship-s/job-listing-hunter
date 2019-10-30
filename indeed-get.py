import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import pandas as pd
import os
import pymysql
import datetime
import hashlib
import time
import json
from pathlib import Path

with open(str(Path(__file__).resolve().parent)+'\credentials.json') as json_file:
    credentials = json.load(json_file)

titles = ['data scientist', 'data engineer', 'data analyst']
locations = ['salt lake city', 'provo']
page = '0'
jobs = []
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M")
today = datetime.date.today()

query = (
        "INSERT INTO indeed (`key`, `time`, `Company_Name`, `Job_Title`, `Query`, `Location`, `datekey`) VALUES (%s, %s, %s, %s, %s, %s, %s)")


def process_page(location, title, page):

    URL = 'https://www.indeed.com/jobs?q=' + title + '&l=' + location + '&start=' + page
    print(URL)
    soup = BeautifulSoup(urllib.request.urlopen(URL).read(), 'html.parser')

    this_page = []

    next_page = soup.find_all('div', attrs={"class": "pagination"})
    for x in next_page:
        page_check = []
        text = x.find_all('span', attrs={"class": "np"})
        for y in text:
            page_check.append(y)

    results = soup.find_all('div', attrs={'data-tn-component': 'organicJob'})

    for x in results:

        this_job = []

        company = x.find('span', attrs={"class": "company"})
        if company:
            this_job.append(company.text.strip())

        job = x.find('a', attrs={'data-tn-element': "jobTitle"})
        if job:
            this_job.append(job.text.strip())

        this_page.append(this_job)

    print(page_check)
    print(len(page_check))

    jobs.append(this_page)
    if len(page_check) < 2 and int(page) != 0:

        return False
    else:
        return True

    print ('----------')


for location in locations:
    for title in titles:
        this_run = title+'_in_'+location
        while process_page(urllib.parse.quote_plus(location), urllib.parse.quote_plus(title), page) is True:
            page = str(int(page) + 10)
            print(page)

        print(jobs)
        jobs_2 = []

        for pages in jobs:
            for x in pages:

                print(x)
                while True:
                    try:
                        connection = pymysql.connect(user=credentials['user'],
                                                     password=credentials['password'],
                                                     host=credentials['host'],
                                                     database=credentials['database'])
                        cursor = connection.cursor()
                        hashing = x[0] + x[1]
                        hashing = hashlib.md5(hashing.encode())
                        hashing = hashing.hexdigest()
                        datekey = x[0] + x[1] + str(today)
                        datekey = hashlib.md5(datekey.encode())
                        datekey = datekey.hexdigest()
                        print(hashing)
                        values = hashing, now, x[0], x[1], title, location, datekey
                        cursor.execute(query, values)
                        cursor.close()
                        connection.commit()
                        connection.close()
                    except pymysql.err.IntegrityError:
                        print('int error')
                        print(values)
                        time.sleep(10)
                        break
                    except pymysql.err.OperationalError:
                        time.sleep(30)
                        print('opp error')
                        continue
                    else:
                        break

        page = '0'














