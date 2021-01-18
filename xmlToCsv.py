#!/usr/bin/env python
# coding: utf-8

# # Installation

# In[95]:


pip install xml.dom.minidom 


# In[94]:


pip install boto3


# In[98]:


#Python code to illustrate parsing of XML files 
# importing the required modules 

import csv 
import requests 
import xml.etree.ElementTree as ET 
from urllib.request import urlopen
from zipfile import ZipFile
import xml.dom.minidom as md
import pandas as pd

import boto 
from boto.s3.key import Key
import boto3

from botocore.exceptions import NoCredentialsError


# In[99]:


# 1: Download the xml from this link


# In[100]:


def getXML():
    try:
        url = 'https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100'

        # creating HTTP response object from given url 
        resp = requests.get(url) 

            # saving the xml file 
        with open('csv1.xml', 'wb') as f: 
                f.write(resp.content) 
    except:
        print('error while getting xml')


# In[101]:


# 2:From the xml, please parse through to the first download link whose   file_type is DLTINS 


# In[134]:


# create element tree object 
def getDownloadLink():
    try:
        tree = ET.parse('csv1.xml') 
        # get root element 
        root = tree.getroot() 
        element=''
        for item in root.findall('./result/doc'):

            for child in item:
                if child.attrib['name']=='file_type' and child.text == 'DLTINS':
                    element=item
                    break
        for child in element:
             if child.attrib['name']=='download_link' :
                    download_link=child.text
        return download_link
    except:
        print('error while getting download link of final xml')


# In[135]:


#download the zip Extract the xml from the zip.


# In[137]:


def downloadAndExtractZip(download_link):
    try:
        zipurl = download_link
            # Download the file from the URL
        zipresp = urlopen(zipurl)
            # Create a new file on the hard drive
        tempzip = open("tempfile.zip", "wb")
            # Write the contents of the downloaded file into the new file
        tempzip.write(zipresp.read())
            # Close the newly-created file
        tempzip.close()
            # Re-open the newly-created file with ZipFile()
        zf = ZipFile("tempfile.zip")
            # Extract its contents into <extraction_path>
            # note that extractall will automatically create the path
        zf.extractall()
            # close the ZipFile instance
        zf.close()
    except:
        print('error while downloading zip and extracting it')


# In[ ]:





# In[107]:


#Convert the contents of the xml into a CSV with the following header:


# In[125]:


def xmltocSV():
    try:
        file = md.parse( "DLTINS_20200108_03of03.xml" )  
        collection = file.documentElement
        # Get all the movies in the collection
        row = collection.getElementsByTagName("FinInstrmGnlAttrbts")
        columnIssr=collection.getElementsByTagName("Issr")

        Issr=[]
        for values in columnIssr:
           Issr.append( values.firstChild.nodeValue)
        data=[]
        for values in row:

           id = values.getElementsByTagName('Id')[0]
           FullNm = values.getElementsByTagName('FullNm')[0].childNodes[0].data
           ClssfctnTp = values.getElementsByTagName('ClssfctnTp')[0].childNodes[0].data
           CmmdtyDerivInd = values.getElementsByTagName('CmmdtyDerivInd')[0].childNodes[0].data
           NtnlCcy = values.getElementsByTagName('NtnlCcy')[0].childNodes[0].data
           data.append([id,FullNm,ClssfctnTp,ClssfctnTp,NtnlCcy])

        df=pd.DataFrame(data,columns=[
                                'FinInstrmGnlAttrbts.Id'
                                ,'FinInstrmGnlAttrbts.FullNm'
                                ,'FinInstrmGnlAttrbts.ClssfctnTp'
                                ,'FinInstrmGnlAttrbts.CmmdtyDerivInd'
                                ,'FinInstrmGnlAttrbts.NtnlCcy'
                                ])
        df['Issr']=Issr
        df.to_csv('AWS.csv')
    except:
        print('Error while xml to csv conversion')


# In[126]:


# Store the csv from step 4) in an AWS S3 bucket


# In[127]:


def upload_to_aws(local_file, bucket, s3_file):
           s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)

           try:
               s3.upload_file(local_file, bucket, s3_file)
               print("Upload Successful")
               return True
           except FileNotFoundError:
               print("The file was not found")
               return False
           except NoCredentialsError:
               print("Credentials not available")
               return False
           
           
def storeInAWSS3():
       # require payment details so not safe, user can use there own key
       ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXXXX'
       SECRET_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

       uploaded = upload_to_aws('AWS.csv', 'bucket_name', 's3_file_name')


# In[128]:


#The above function should be run as an AWS Lambda (Optional)


# In[129]:


def Handler():
    import os
    import json

    def lambda_handler(event, context):
        json_region = os.environ['AWS_REGION']
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "Region ": json_region
            })
        }


# # PROCESS

# In[131]:


def main():
    xml=getXML()
    download_link=getDownloadLink()
    downloadAndExtractZip(download_link)
    xmltocSV()


# In[132]:


main()


# In[ ]:




