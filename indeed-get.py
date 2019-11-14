import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import pymysql
import datetime
import hashlib
import time
import json
from pathlib import Path

# where to load the database login credentials from for storing the data the scraper grabs (in JSON format)
with open(str(Path(__file__).resolve().parent) + '\credentials.json') as json_file:
    credentials = json.load(json_file)

'''user defined parameters, in case this needs to be updated for a different location or title'''
titles = ['data scientist', 'data engineer', 'data analyst']
locations = ['salt lake city']

page = '0'
jobs = []
# definition of the start date of the script run, for the sake of tracking individual days
now = datetime.datetime.now()
now = now.strftime("%Y-%m-%d %H:%M")
today = datetime.date.today()

# the SQL query to write to the database
query = (
        "INSERT INTO indeed (`key`, `time`, `Company_Name`, `Job_Title`, `Query`, `Location`, `datekey`) VALUES (%s, %s, %s, %s, %s, %s, %s)")


def process_page(location, title, page):
    '''
    Scrapes a given page on Indeed, based on the user parameters defined above

    :param location: geolocation to search in
    :param title: job title to search for
    :param page: current pagination number being scraped
    :return: True while pages which have not been scraped still exist
    '''

    # URL concatination for request
    URL = 'https://www.indeed.com/jobs?q=' + title + '&l=' + location + '&start=' + page
    print(URL)
    soup = BeautifulSoup(urllib.request.urlopen(URL).read(), 'html.parser')

    # define a temporary list to hold all the job listings on a page
    this_page = []

    # discover if the 'next page' button exists on the page, meaning there are more pages remaining in this query
    next_page = soup.find_all('div', attrs={"class": "pagination"})
    for x in next_page:
        page_check = []
        text = x.find_all('span', attrs={"class": "np"})
        for y in text:
            page_check.append(y)

    # find all the individual job listings on a page
    results = soup.find_all('div', attrs={'data-tn-component': 'organicJob'})

    # scan the job listing for details
    for x in results:

        this_job = []

        company = x.find('span', attrs={"class": "company"})
        if company:
            this_job.append(company.text.strip())

        job = x.find('a', attrs={'data-tn-element': "jobTitle"})
        if job:
            this_job.append(job.text.strip())

        this_page.append(this_job)

    # append each page to master list of jobs
    jobs.append(this_page)

    # carry on to next query if no more pagination exists
    if len(page_check) < 2 and int(page) != 0:

        return False
    else:
        return True


def indeed_get():
    """
    loop through all user defined parameters and write them to the user-defined database
    """
    for location in locations:
        for title in titles:
            while process_page(urllib.parse.quote_plus(location), urllib.parse.quote_plus(title), page) is True:
                page = str(int(page) + 10)

            print(jobs)

            for pages in jobs:
                for x in pages:

                    print(x)
                    while True:
                        # write each job to the database
                        try:
                            # connect to MySQL database
                            connection = pymysql.connect(user=credentials['user'],
                                                         password=credentials['password'],
                                                         host=credentials['host'],
                                                         database=credentials['database'])
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
                            break

                        except pymysql.err.OperationalError:
                            # OperationalError means an error connecting to the database, hopefully temporary, try again
                            time.sleep(30)
                            print('opp error')
                            continue
                        else:
                            break

            page = '0'  # reset page number for subsequent query loops


if __name__ == "__main__":
    indeed_get()