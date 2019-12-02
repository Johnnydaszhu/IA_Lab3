
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
import requests
import dataframe
from bs4 import BeautifulSoup
from tabulate import tabulate
from selenium import webdriver


#call the webdriver
browser = webdriver.Chrome("C:/Program Files (x86)/Microsoft Visual Studio/Shared/Python37_64/chromedriver.exe")

#enter the url path that needs to be accessed by webdriver
browser.get('https://www.charitiesnys.com/RegistrySearch/search_charities.jsp')

#identify xpath of location to select element
inputElement = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[2]/td[2]/input[1]")
inputElement.send_keys('0')
inputElement1 = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[10]/td/input[1]").click()


#identify the table to scrape
table = browser.find_element_by_css_selector('table.Bordered')

#prepare csv file name
datetime = time.strftime("%Y%m%d-%H%M%S")
filename = 'anynameisok'#specify location of s3:/{my-bucket}/
datetime = time.strftime("%Y%m%d%H%M%S")
filenames3 = "%s%s.csv"%(filename,datetime)

#create empty dataframe
data =[]

#loop through dataframe to export table
for row in table.find_elements_by_css_selector('tr'):
      cols = data.append([cell.text for cell in row.find_elements_by_css_selector('td')])
#print(data)

#update dataframe with header
data = pd.DataFrame(data, columns = ["Organization Name", "NY Reg #", "EIN" ,"Registrant Type","City","State"])
#print(data)

#upload to s3
data.to_csv(filenames3, header=True, line_terminator='\n')

S3 = boto3.client('s3')
SOURCE_FILENAME = filenames3
BUCKET_NAME = '<BUCKET_NAME>'

# Uploads the given file using a managed uploader, which will split up large
# files automatically and upload parts in parallel.
S3.upload_file(SOURCE_FILENAME, BUCKET_NAME, SOURCE_FILENAME)

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
import requests
import dataframe
from bs4 import BeautifulSoup
from tabulate import tabulate
from selenium import webdriver


#call the webdriver
browser = webdriver.Chrome("C:/Program Files (x86)/Microsoft Visual Studio/Shared/Python37_64/chromedriver.exe")

#enter the url path that needs to be accessed by webdriver
browser.get('https://www.charitiesnys.com/RegistrySearch/search_charities.jsp')

#identify xpath of location to select element
inputElement = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[2]/td[2]/input[1]")
inputElement.send_keys('0')
inputElement1 = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[10]/td/input[1]").click()


#identify the table to scrape
table = browser.find_element_by_css_selector('table.Bordered')

#prepare csv file name
datetime = time.strftime("%Y%m%d-%H%M%S")
filename = '<s3folderpath>'#specify location of s3:/{my-bucket}/
datetime = time.strftime("%Y%m%d%H%M%S")
filenames3 = "%s%s.csv"%(filename,datetime)

#create empty dataframe
data =[]

#loop through dataframe to export table
for row in table.find_elements_by_css_selector('tr'):
      cols = data.append([cell.text for cell in row.find_elements_by_css_selector('td')])
#print(data)

#update dataframe with header 
data = pd.DataFrame(data, columns = ["Organization Name", "NY Reg #", "EIN" ,"Registrant Type","City","State"])
#print(data)

#upload to s3
data.to_csv(filenames3, header=True, line_terminator='\n')

