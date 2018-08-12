import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from newsapi import NewsApiClient
from flatten_dict import flatten
import os
import csv
import boto3
import re


class Utility:

    def __init__(self,news_api_key,s3_bucket):
        self.news_api_key=news_api_key
        self.s3_bucket=s3_bucket
    def __repr__(self):
        return f'Utility class with api key{self.news_api_key},and bucket{self.s3_bucket}'

# method getSources()
# params languages as comma separated string for example 'en' or 'en,es,fr'
# returns a list of sources as comma separated string for example 'abc,abc-us,bbc' 
    def getSources(self,category=None,language=None, country=None):
        cat=category
        lang=language
        count=country
        newsapi = NewsApiClient(api_key=self.news_api_key)
        allSources = newsapi.get_sources(category=cat,language=lang,country=count) 
        # create a comma separated string from allsources dict
        sourceList = []
        for source in allSources['sources']:
            sourceList.append(source['id'])
        sourcesString = ",".join(sourceList)# comma seperated string of sources from sourceList
        return sourcesString


# method getheadlines()
# params sourcesString is a string of comma separated sources for example 'abc,abc-us,bbc'
# returns a list of csvfilenames for example ['abc.csv', 'abc-us.csv', 'bbc.csv']
    def getheadlines(self,sourcesString):
	    #top_headlines = newsapi.get_top_headlines(sources=sourcesString)# top_headlines is a dict
        newsapi = NewsApiClient(api_key=self.news_api_key)
        top_headlines = newsapi.get_top_headlines(sources=sourcesString)# top_headlines is a dict
        sourcesList = sourcesString.split(',')
        articlesListBySource = []
        for source in sourcesList:
            articlesList = []
            articleSourceName = ''
            for article in top_headlines['articles']: # for each article 
                if article['source']['id'] == source:
                    articleSourceName = article['source']['name']
                    articlesList.append(flatten(article, reducer='path')) # flatten and append to articlesList
            if articleSourceName != '':  # append only if articleSourceName 
                articlesListBySource.append({'sourceName':articleSourceName, 'articlesList':articlesList})
        csvFilesList = []
        for source in articlesListBySource: # for each source
            columns = set().union(*(d.keys() for d in source['articlesList']))
            csvFilesList.append(re.sub('[^a-zA-Z0-9]','_',source['sourceName'])+'.csv')
            with open(re.sub('[^a-zA-Z0-9]','_',source['sourceName'])+'.csv', 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, columns)
                writer.writeheader()
                for article in source['articlesList']:
                    writer.writerow(article)
        return csvFilesList


# method uploadCsvToS3()
# params csvList as a list of csv names for example ['abc.csv','abc_us.csv','bbc.csv']
# upload files to s3 bucket mentioned in the config 
    def uploadCsvToS3(self,csvList):
        s3 = boto3.client('s3')
        for item in csvList:
            now=dt.datetime.now().strftime('%m-%d-%Y %H:%M:%S')
            output= item[:-3]+'/'+now+'_top_headlines.csv'
            with open(item, 'rb') as data:
                s3.upload_fileobj(data, self.s3_bucket, output)
            os.remove(item)


# method getheadlineswithkeywords()
# params keywords is string of  comma seperated keywords for example 'cancer,tempus,eric lefkofsky'
# returns a list of csvfilenames for example ['cancer.csv', 'tempus.csv', 'eric_lefkofsky.csv']
    def getheadlineswithkeywords(self,keywords):
        searchList = keywords.split(',')
        listofKeywordAndArticles= []
    
    # for each keyword or searchString fetch headlines 
    # and append flattend articles into listItems as a list of dicts
        for searchString in searchList:
            articlesList = []
            newsapi = NewsApiClient(api_key=self.news_api_key)
            result = newsapi.get_top_headlines(q=searchString)
            if len(result['articles']) != 0:
                for article in result['articles']:# for each article
                    articlesList.append(flatten(article, reducer='path'))# flatten and append to articlesList
            listofKeywordAndArticles.append({'searchTerm': searchString, 'articlesList': articlesList})

    # create a csv of top headlines for each keyword
    # make list of csv filenames in csvFilesList
        csvFilesList = []
        for keywordArticles in listofKeywordAndArticles: # for each keyword
            columns = set().union(*(d.keys() for d in keywordArticles['articlesList']))
            csvFilesList.append((keywordArticles['searchTerm']).replace(" ", "_")+'.csv')
            with open((keywordArticles['searchTerm']).replace(" ", "_")+'.csv', 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, columns)
                writer.writeheader()
                for article in keywordArticles['articlesList']:
                    writer.writerow(article)
        return csvFilesList
