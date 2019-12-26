"""
Save scraped data to mysql DB

"""

import mysql.connector as mc
import pandas as pd

# =============================================================================
# Required fields to connect to DB
# =============================================================================
host_name = 'remotemysql.com'
dbname = 'dbname'
port = '0000'
user_name = 'username' #enter username
pwd = 'pasword' #enter your database password

# =============================================================================
# Load data, replase NAs with ' '
# =============================================================================
job_positions = pd.read_csv('./results/jobs_2019_12_de.csv')
job_positions.fillna(' ', inplace=True)

# =============================================================================
# Connect to DB
# =============================================================================
try:
    conn = mc.connect(host=host_name,database=dbname,user=user_name,password=pwd,port=port)
except mc.Error as e:
    raise e
else:
    print('\n\nConnected!\n\n')

# =============================================================================
# Fill in TABLE in DB using SQL
# =============================================================================
try:    
    cur = conn.cursor()

    for i in job_positions.index:
        
        sql = 'INSERT INTO jobs (jobkey, search_title, job_title, \
                                company, location, date, description) \
                            VALUES {}'.format(tuple(job_positions.loc[i, :].values))
        
        try:
            cur.execute(sql)
        except mc.IntegrityError as e:
            print(e)
    # commit changes    
    conn.commit()    
    
finally:
    #close the connection
    cur.close()
    conn.close()         
