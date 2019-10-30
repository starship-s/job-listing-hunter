from bs4 import BeautifulSoup
import datetime
import requests
import pymysql
import datetime
import hashlib
import time
import json
from pathlib import Path

with open(str(Path(__file__).resolve().parent)+'\credentials.json') as json_file:
    credentials = json.load(json_file)

titles = ['data scientist', 'data engineer', 'data analyst']
locations = ['salt lake city']
page = '1'
jobs = []
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M")
today = datetime.date.today()
headers = {'User-Agent': 'Mozilla/5.0'}

query = (
        "INSERT INTO glassdoor (`key`, `time`, `Company_Name`, `Job_Title`, `Query`, `Location`, `datekey`) VALUES (%s, %s, %s, %s, %s, %s, %s)")


def process_page(location, title, page):
    url_indexing = len(title) + 15
    url = 'https://www.glassdoor.com/Job/' + location + '-' + title + '-jobs-SRCH_IL.0,14_IC1128289_KO15,' + \
          str(url_indexing)+'_IP' + str(page) + '.htm'
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    this_page = []

    results = soup.find_all('div', attrs={'class': 'jobContainer'})
    last_page = soup.find_all('li', attrs={'class': 'page current last'})

    for x in results:
        this_job = []

        company = x.find('div', attrs={"class": "jobInfoItem jobEmpolyerName"}).text.strip()
        job_title = x.find_all('a', attrs={"class": "jobTitle"})[1].text.strip()
        this_job.append(company)
        this_job.append(job_title)

        this_page.append(this_job)

    jobs.append(this_page)

    print(url)

    if len(last_page) == 0:
        return True
    else:
        return False


for location in locations:
    for title in titles:
        while process_page(location.replace(' ', '-'), title.replace(' ', '-'), page) is True:
            page = str(int(page) + 1)

        for pages in jobs:
            for x in pages:

                connection = pymysql.connect(user=credentials['user'],
                                             password=credentials['password'],
                                             host=credentials['host'],
                                             database=credentials['database'])

                print(x)
                while True:
                    try:
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
                        time.sleep(30)
                        break
                    except pymysql.err.OperationalError:
                        time.sleep(30)
                        print('opp error')
                        continue
                    else:
                        break

        page = '1'


print(jobs)


