"""
Scraping job posts from indeed

"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

from datetime import date


# =============================================================================
# Web Scraping of search results from de.indeed.com
# =============================================================================

# Positions to search

positions = ["data+analyst", "data+scientist", "data+engineer",
             "business+intelligence+analyst", "bi+analyst"]

# since when
fromage = 30

# =============================================================================
# "any" => all time
# 1 => last day
# 15 => last 14 days
# =============================================================================

# search results per page
limit = 50  
 
URLs = []

for position in positions:
    
    start = 0
    
    URL = 'https://de.indeed.com/jobs?'+ \
           'q=title%3A"'+position+'"&'+ \
           'l=Deutschland'+'&'+ \
           'radius=0'+'&'+ \
           'limit='+str(limit)+'&'+ \
           'fromage='+str(fromage)+'&'+\
           '&start='+str(start)
       
    try:
        page = requests.get(URL)
        page.raise_for_status()
    
        soup = BeautifulSoup(page.text, 'html.parser')
       
    except Exception as exc:
        print('There was a problem: %s' % (exc))
        
    #print(soup.prettify())
    
    # get the number of found job positions
    searches = soup.find_all(name='div', attrs={'id':'searchCount'})
    
    if searches != []:
        jobs_count = int(searches[0].text.strip().split()[3])
        
    else:
        jobs_count = 0
    
    print('Position: {}, jobs {}'.format(position, jobs_count))

    # get the URLs for all pages in search results
    # Variable 'limit' contains a number of search results per page
    
    # Variable 'start' encodes a number of a search page
    # start = 0 first page
    # start = limit second page
    # start = 2*limit third page 

    URLs.append((position, URL))
        
    for start in range(limit, jobs_count, limit):
        
    
        URL = 'https://de.indeed.com/jobs?'+ \
              'q=title%3A"'+position+'"&'+ \
              'l=Deutschland'+'&'+ \
              'radius=0'+'&'+ \
              'limit='+str(limit)+'&'+ \
              'fromage='+str(fromage)+'&'+\
              '&start='+str(start)
        
        URLs.append((position, URL))
    
# =============================================================================
# Job Keys
# =============================================================================

def extract_jobkeys(soup):
    
    jobkeys = []
    
    divs = soup.find_all('div')
    
    for div in divs:
        
        jobkey = div.get("data-jk")
        if jobkey != None:
            jobkeys.append(div.get("data-jk"))
        
    return jobkeys

# =============================================================================
# Get full job description by its key from job search results 
# =============================================================================

def extract_description(soup): 
    
    div = soup.find('div', attrs={'id':'desc'})
    
    string = str(div)
       
    summary = re.sub(r'<.*?>', ' ', string)   

    return summary

# =============================================================================
# Collect data from every link in search results    
# =============================================================================
job_positions = pd.DataFrame(columns = ['jobkey',
                                        'position',
                                        'jobtitle',
                                        'company',
                                        'location',
                                        'date',
                                        'description'])    

for URL in URLs:
    
    page = requests.get(URL[1])
    
    time.sleep(1)
    
    soup = BeautifulSoup(page.text, 'html.parser')
    
    for jk in extract_jobkeys(soup):
        
        if jk not in job_positions['jobkey'].values:
        
            post = soup.find("div", 
                                 attrs = {'data-jk':jk})
            
            title = post.find("a", 
                              attrs = {'data-tn-element':'jobTitle'})['title']
            
            company = post.find("span", 
                                attrs = {'class':'company'})
            if company != None:
                company = company.text.strip()
                            
            location = post.find("div", 
                                 attrs = {'class':'recJobLoc'})['data-rc-loc']
        
            url_viewjob = "https://de.indeed.com/m/viewjob?jk="+jk
    
            page = requests.get(url_viewjob)
    
            time.sleep(1)

            viewjob = BeautifulSoup(page.content, 'html.parser')
        
            description = extract_description(viewjob)
    
            job_positions.loc[len(job_positions), :] = [jk,
                                                        URL[0].replace('+', ' '), # search position
                                                        title,
                                                        company,
                                                        location,
                                                        str(date.today()),
                                                        description]

# Save data to file
job_positions.to_csv("./Results/jobs" +\
                     '_' + str(date.today().year) +\
                     '_' + str(date.today().month) + "_de.csv", index=False)


