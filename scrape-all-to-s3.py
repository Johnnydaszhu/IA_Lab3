####
#Author: brandon chiazza
#version 1.0
#references:
#https://www.programiz.com/python-programming/working-csv-files
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
#CLI aws s3api create-bucket --bucket my-bucket-name --region us-west-2 --create-bucket-configuration LocationConstraint=us-west-2
#https://realpython.com/python-boto3-aws-s3/
#https://robertorocha.info/setting-up-a-selenium-web-scraper-on-aws-lambda-with-python/
##

import awscli
import sys
import selenium
import unittest
import boto3
import pandas as pd
import tabulate
import time
import request
from bs4 import BeautifulSoup
from tabulate import tabulate
from selenium import webdriver
import time
import random


#call the webdriver
browser = webdriver.Chrome("/Users/jz/Downloads/chromedriver")

def table_to_dataframe(table):
    #create empty dataframe
    data =[]

    #loop through dataframe to export table
    for row in table.find_elements_by_css_selector('tr'):
          cols = data.append([cell.text for cell in row.find_elements_by_css_selector('td')])
    #print(data)

    #update dataframe with header
    data = pd.DataFrame(data, columns = ["Organization Name", "NY Reg #", "EIN" ,"Registrant Type","City","State"])

    return data

#prepare csv file name
datetime = time.strftime("%Y%m%d-%H%M%S")
filename = 'info-scrape'#specify location of s3:/{my-bucket}/
datetime = time.strftime("%Y%m%d%H%M%S")
filenames3 = "%s%s.csv"%(filename,datetime)


data_list = []

for i in range(1,8):
        #click next page
        browser.get(f"https://www.charitiesnys.com/RegistrySearch/search_charities_action.jsp?orgName=&d-49653-p={i}&city=&searchType=contains&reg1=&project=Charities&reg3=&reg2=&ein=0-&orgId=&num1=0&state=none&regType=ALL&num2=")
        #identify the table to scrape
        data_list.append(table_to_dataframe(browser.find_element_by_css_selector('table.Bordered')))
        time.sleep(random.random())

df = pd.concat(data_list).drop_duplicates().dropna().reset_index(drop=True)
browser.close()
df.to_csv(filenames3, header=True, line_terminator='\n')

S3 = boto3.client('s3')
SOURCE_FILENAME = filenames3
BUCKET_NAME = 'info-scrape'

# Uploads the given file using a managed uploader, which will split up large
# files automatically and upload parts in parallel.
S3.upload_file(SOURCE_FILENAME, BUCKET_NAME, SOURCE_FILENAME)
