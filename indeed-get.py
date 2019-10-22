import urllib.request
from bs4 import BeautifulSoup

URL = "https://www.indeed.com/jobs?q=data+scientist+%2420%2C000&l=New+York&start=10"

soup = BeautifulSoup(urllib.request.urlopen(URL).read(), 'html.parser')

results = soup.find_all('div', attrs={'data-tn-component': 'organicJob'})

for x in results:

    company = x.find('span', attrs={"class":"company"})
    if company:
        print('company:', company.text.strip() )

    job = x.find('a', attrs={'data-tn-element': "jobTitle"})
    if job:
        print('job:', job.text.strip())

    salary = x.find('nobr')
    if salary:
        print('salary:', salary.text.strip())

    print ('----------')