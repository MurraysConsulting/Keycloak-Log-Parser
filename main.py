#import support
import pandas as pd
import os
import glob
from datetime import datetime as dt
import json


# sets the folder path to collect log data.
path1 = os.getcwd()

#this is assuming that a subfolder is created to hold the raw data that will be imported in a folder named data
path2 = path1 + "\\Data\\"

#sets the raw imports.
data_list = glob.glob(os.path.join(path2, "*keycloak*.json"))

#display the list of files that will be imported to review
print('List of imported files:')
count = 0
for data in data_list:
    print(count, ': ', data.split('\\')[-1])
    count+=1


# open files

content = []
for data in data_list:
    try:
        with open(data) as f:
            content.extend(json.load(f))
    except:
        #Prints which file failed to open, as well as gives you a location to go test why the json load possibly failed.
        print ("Failed: " + data)
        print ("Go to https://jsonlint.com/ to fix the error")

#Displays the total number of objects to be processed.
print("Total entries: " + len(content))

#start to parse and clean the data
#set the variables to process the log data
log_db = []
count = 0
tempDate = str
tempTime = str
logKeys = ['type', 'clientId', 'userId', 'grant_type', 'username', 'identity_provider_identity']

idstore = []


for entry in content:
    tempLog = {}
    try:
        #sets the primary key as the time
        tempLog['id'] = entry['@message']['json']['time']

        #pulls out of the log only the json item from the entry that is already cleaned for parsing
        logitem = entry['@message']['json']['log'].split(',')

        #sets the time up to be split to be used to set the date information for pivot tables.
        tempDate, tempTime = entry['@message']['json']['time'].split('T')

        tempLog['year'] = int(tempDate.split('-')[0]) 
        tempLog['month'] = int(tempDate.split('-')[1])
        tempLog['day'] = int(tempDate.split('-')[2])        
        tempLog['time'] = tempTime.split('Z')[0]

        #takes the information from the json log info
        for item in logitem:
            if '=' not in item:
                pass
            else:
                item = item.split('=')
                item[0] = item[0].split(' ')[-1]
                if item[0] in logKeys:
                    tempLog[item[0]]= item[1]
        if 'type' in tempLog.keys():
            if tempLog['id'] not in idstore:
                idstore.append(tempLog['id'])
                log_db.append(tempLog)
    except:
        print('Failed entry: ' + entry['@message'])
        pass
    count += 1

#convert to a dataframe to prepare to export to excel
df_loginfo = fu.DataFrame(log_db)

#set the output file name with a time stamp as part of the output
file_output = dt.now()

file_output = file_output.strftime('%Y%m%d') + '_UserInfo.xlsx'

#output the file.
df_loginfo.to_excel(file_output, sheet_name= 'logs', index=False)
