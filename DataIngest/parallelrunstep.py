import argparse
import os
import pandas as pd
import time
import datetime
from datetime import timedelta
#from ./DataImport/helperFunctions import pullUsageAndSaveV2,authenticate_client_key
import adal, uuid, time
import requests
import json
import pandas as pd


import datetime 
from datetime import timedelta
import pandas as pd
from azureml.core import Workspace, Datastore, Experiment,Dataset, Experiment, Run
from azureml.core.authentication import ServicePrincipalAuthentication



def authenticate_client_key():
    """
    Authenticate using service principal w/ key.
    """
    authority_host_uri = 'https://login.microsoftonline.com'
    tenant = '3251a1be-1e1a-4add-894b-c15d0dd4d342'
    authority_uri = authority_host_uri + '/' + tenant
    resource_uri = 'https://management.core.windows.net/'
    client_id = '396ce7d2-4cb5-41c7-8583-dce9f23423e5'
    run = Run.get_context()
    client_secret = run.get_secret(name="aadappsecret")

    context = adal.AuthenticationContext(authority_uri, api_version=None)
    mgmt_token = context.acquire_token_with_client_credentials(resource_uri, client_id, client_secret)
    return mgmt_token["accessToken"]




def pullUsageAndSaveV2(url, token,startDate,endDate, counter,usageDataFrame):
    #print (url)


    subscription_id = 'bd04922c-a444-43dc-892f-74d5090f8a9a'
    resource_group = 'mlplayarearg'
    workspace_name = 'testdeployment'
    run = Run.get_context()
    client_secret = run.get_secret(name="aadappsecretAML")

    svc_pr = ServicePrincipalAuthentication(
       tenant_id="72f988bf-86f1-41af-91ab-2d7cd011db47",
       service_principal_id="8e1d74de-d84f-4971-b737-66e737b636c1",
       service_principal_password=client_secret)


    workspace = Workspace(subscription_id, resource_group, workspace_name,auth=svc_pr)

    mydatastore = Datastore.get(workspace, 'billingdatablobstorage')

    print('about to make request to get usage')
    resp = requests.get(url, headers = {"Authorization":"Bearer " + token})
    if resp.status_code == 401:
        token = authenticate_client_key()
    allData = resp.content

    dataObj = json.loads(allData)
    if 'value' not in dataObj:
        return
        
    usageData = dataObj['value']
    
    if 'properties' in usageData:
        usageDF = pd.DataFrame.from_dict(usageData)        
        propsExpanded = usageDF['properties'].apply(pd.Series)
        usageDFNew = pd.concat([usageDF.drop(['properties'], axis=1),propsExpanded[['meterId','resourceGroup','offerId','chargeType','frequency','quantity','effectivePrice','cost','unitPrice','billingCurrency','date','resourceId']]], axis=1)
        print(usageDFNew.shape)
        files = []
        groupedFiles = []
        if (usageDataFrame.shape[0] == 0):
            print('assigning data first time')
            usageDataFrame = usageDFNew
        else:
            print(f'adding {usageDFNew.shape[0]} rows to exisitng dataframe of size {usageDataFrame.shape[0]}')
            usageDataFrame = usageDataFrame.append(usageDFNew)
            print (f'usageDataFrame is now {usageDataFrame.shape[0]}')
    if 'nextLink' in dataObj:     
        pullUsageAndSave(datapath,dataObj['nextLink'],token,startDate, endDate,counter+1,usageDataFrame)
    else:
        print (f'saving dataframe with shape: {usageDataFrame.shape}')
        for singleDay in usageDataFrame['date'].unique():
            singleDayData = usageDataFrame[usageDataFrame['date'] == singleDay]
            print (f'saving rows for {singleDay} {singleDayData.shape[0]}')
            singleDayData.to_csv(f"{singleDay[0:4]}{singleDay[5:7]}{singleDay[8:10]}.csv")
            files.append(f"{singleDay[0:4]}{singleDay[5:7]}{singleDay[8:10]}.csv")
            mydatastore.upload_files(
                    files, # List[str] of absolute paths of files to upload
                    target_path='rawdata',
                    overwrite=True,
                    )
            groupedData = singleDayData.groupby(['meterId','resourceGroup','date']).agg({'cost':sum,'quantity':sum})
            groupedData.to_csv(f"{singleDay[0:4]}{singleDay[5:7]}{singleDay[8:10]}grouped.csv")
            groupedFiles.append(f"{singleDay[0:4]}{singleDay[5:7]}{singleDay[8:10]}grouped.csv")
            mydatastore.upload_files(
                    groupedFiles, # List[str] of absolute paths of files to upload
                    target_path='rolledup',
                    overwrite=True,
                    )
    

def init():
    os.chdir(os.path.dirname(__file__))
    global token
    print('this is just testing if it works')
    token = authenticate_client_key()

    print(token)
    


def run(mini_batch):

    for file_path in mini_batch:
        print (file_path)
        input_data = pd.read_csv(file_path)
        for index, row in input_data.iterrows():
            print(row)
            startDate = row['startDate']
            endDate = row['endDate']
            #startDate = startDate.strftime("%Y-%m-%d")
            #endDate = endDate.strftime("%Y-%m-%d")
            url = f"https://management.azure.com/subscriptions/c79c9d40-d5f3-40ee-bb7b-e43b3bb4efe6/providers/Microsoft.Consumption/usagedetails?api-version=2019-11-01&$filter=properties/usageStart+ge+'{startDate}T00:00:00'+and+properties/usageStart+le+'{endDate}T00:00:00'"
            print(url)
            pullUsageAndSaveV2(url, token,startDate  ,endDate ,0,pd.DataFrame())
    return input_data
