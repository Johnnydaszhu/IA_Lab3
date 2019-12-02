master
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
import csv
# import dataframe
from bs4 import BeautifulSoup
from tabulate import tabulate
from selenium import webdriver


#call the webdriver
browser = webdriver.Chrome("/Users/jz/Downloads/chromedriver")

#enter the url path that needs to be accessed by webdriver
browser.get('https://www.charitiesnys.com/RegistrySearch/search_charities.jsp')

#identify xpath of location to select element
inputElement = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[2]/td[2]/input[1]")
inputElement.send_keys('0')
inputElement1 = browser.find_element_by_xpath("/html/body/div/div[2]/div/table/tbody/tr/td[2]/div/div/font/font/font/font/font/font/table/tbody/tr[4]/td/form/table/tbody/tr[10]/td/input[1]").click()


#identify the table to scrape
table = browser.find_element_by_css_selector('table.Bordered')

datetime = time.strftime("%Y%m%d-%H%M%S")
filedirectory = '/Users/jz/GitHub/IA_Lab3/'
filename = 'cb_table_'
datetime = time.strftime("%Y%m%d%H%M%S")
file = "%s%s%s.csv"%(filedirectory,filename,datetime)

#write to csv using opencsv
#prepare csv contraints
csv.register_dialect('myDialect',
delimiter = '|',
quoting=csv.QUOTE_ALL,
quotechar = '"',
skipinitialspace=False
,escapechar='\\')

#write schema
schema = ['Organization Name', 'NY Reg #','EIN','Registrant Type','City','State']

#generate file output
with open(file, 'w', newline='\n') as csvfile:
	wr = csv.writer(csvfile, dialect = 'myDialect')
	wr.writerow([cell for cell in schema])
	for row in table.find_elements_by_css_selector('tr'):
		wr.writerows([cell.text for cell in row.find_elements_by_css_selector('td')])

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
import csv
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

datetime = time.strftime("%Y%m%d-%H%M%S")
filedirectory = '<file outputpath>'
filename = 'cb_table_'
datetime = time.strftime("%Y%m%d%H%M%S")
file = "%s%s%s.csv"%(filedirectory,filename,datetime)

#write to csv using opencsv
#prepare csv contraints
csv.register_dialect('myDialect',
delimiter = '|',
quoting=csv.QUOTE_ALL,
quotechar = '"',
skipinitialspace=False
,escapechar='\\')

#write schema
schema = ['Organization Name', 'NY Reg #','EIN','Registrant Type','City','State']

#generate file output
with open(file, 'w', newline='\n') as csvfile:
	wr = csv.writer(csvfile, dialect = 'myDialect')
	wr.writerrow([cell for cell in schema])
	for row in table.find_elements_by_css_selector('tr'):
		wr.writerows([cell.text for cell in row.find_elements_by_css_selector('td')]) 
master
