from bs4 import BeautifulSoup
import requests
import pymysql
import datetime
import hashlib
import time
import json
from pathlib import Path

# where to load the database login credentials from for storing the data the scraper grabs (in JSON format)
with open(str(Path(__file__).resolve().parent)+'\credentials.json') as json_file:
    credentials = json.load(json_file)

'''user defined parameters, in case this needs to be updated for a different location or title'''
titles = ['data scientist', 'data engineer', 'data analyst']
locations = ['salt lake city']

page = '1'
jobs = []

# definition of the start date of the script run, for the sake of tracking individual days
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M")
today = datetime.date.today()

# Glassdoor doesn't like to work without spoofing proper headers
headers = {'User-Agent': 'Mozilla/5.0'}

# the SQL query to write to the database
query = (
        "INSERT INTO glassdoor (`key`, `time`, `Company_Name`, `Job_Title`, `Query`, `Location`, `datekey`) VALUES (%s, %s, %s, %s, %s, %s, %s)")


def process_page(location, title, page):
    '''
    Scrapes a given page on Glassdoor, based on the user parameters defined above

    :param location: geolocation to search in
    :param title: job title to search for
    :param page: current pagination number being scraped
    :return: True while pages which have not been scraped still exist
    '''

    # Glassdoor pulls some funny tricks with its URL structure, we need to do an index of the length of our search
    url_indexing = len(title) + 15
    url = 'https://www.glassdoor.com/Job/' + location + '-' + title + '-jobs-SRCH_IL.0,14_IC1128289_KO15,' + \
          str(url_indexing)+'_IP' + str(page) + '.htm'
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    # define a temporary list to hold all the job listings on a page
    this_page = []

    # find all results on a given page
    results = soup.find_all('div', attrs={'class': 'jobContainer'})
    # find if this is the final page for this query or not
    last_page = soup.find_all('li', attrs={'class': 'page current last'})

    # scan the job listing for details
    for x in results:

        this_job = []

        company = x.find('div', attrs={"class": "jobInfoItem jobEmpolyerName"}).text.strip()
        job_title = x.find_all('a', attrs={"class": "jobTitle"})[1].text.strip()
        this_job.append(company)
        this_job.append(job_title)

        this_page.append(this_job)

    # append each page to master list of jobs
    jobs.append(this_page)

    print(url)

    # check if this is the last page or not
    if len(last_page) == 0:
        return True
    else:
        return False


def glassdoor_get():
    """
    loop through all user defined parameters and write them to the user-defined database
    """
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
                        # write each job to the database
                        try:
                            # connect to MySQL database
                            cursor = connection.cursor()
                            # creates a hash value which allows me to compare the same job listed across multiple sites
                            hashing = x[0] + x[1]
                            # generated from job title + company name
                            hashing = hashlib.md5(hashing.encode())
                            hashing = hashing.hexdigest()
                            # generates a primary key, because the same job listing sometimes appears on multiple pages
                            datekey = x[0] + x[1] + str(today) + title
                            datekey = hashlib.md5(datekey.encode())
                            datekey = datekey.hexdigest()
                            values = hashing, now, x[0], x[1], title, location, datekey
                            # write to the database
                            cursor.execute(query, values)
                            cursor.close()
                            connection.commit()
                            connection.close()

                        except pymysql.err.IntegrityError:
                            # Integrity Error means the primary key has already appeared in the database, so we can skip
                            print('int error')
                            print(values)
                            time.sleep(10)
                            break

                        except pymysql.err.OperationalError:
                            # OperationalError means an error connecting to the database, hopefully temporary, try again
                            time.sleep(30)
                            print('opp error')
                            continue
                        else:
                            break

            page = '1'  # reset page number for subsequent query loops


if __name__ == "__main__":
    glassdoor_get()


