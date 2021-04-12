import argparse
import os
import pandas as pd
import time
import datetime
from datetime import timedelta
def createnDayBlocks(datapath,startDate,n):
    data = []
    endDate=startDate

    while endDate <  datetime.datetime.now().date():
        endDate = startDate + timedelta(n)
        if(endDate >= datetime.datetime.now().date()):
            print (endDate)
            endDate = datetime.datetime.now().date()
        data.append({'startDate':startDate,'endDate':endDate})  
        startDate = endDate + timedelta(1)
    
    dfToWrite = pd.DataFrame(data)  
    dfToWrite.to_csv(f"{datapath}/daystoprocess/data.csv")


parser = argparse.ArgumentParser("getData")
parser.add_argument("--arg1", type=str, help="input start date")
parser.add_argument("--arg2", type=str, help="datapath to write or read")
args = parser.parse_args()


startDate=args.arg1 #'2020-10-01'
if startDate=='9999-99-99':
    startDate = (datetime.datetime.now().date() + timedelta(-10) ).strftime('%Y-%m-%d')

createnDayBlocks(args.arg2,datetime.datetime.strptime(startDate,'%Y-%m-%d').date(),10)


