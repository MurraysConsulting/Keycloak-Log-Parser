#import support
import pandas as fu
import os
import glob
from datetime import datetime as dt
import json
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo
import string

# This is a look at the first parser:
# sets the folder path to collect log data.
path1 = os.getcwd()
path2 = path1 + "\\Data\\"

# Would like to add in a db function to collect data to the db to reduce the amount of external data loads.  For this would still need to identify the new data being parsed.

#sets the raw imports.
file_list = glob.glob(os.path.join(path2, "*block*.json"))  #This looks at the new format file from Keycloak and collects objects for parsing
print('List of imported files:')  #Show the list of files to be collected.
count = 0
for file in file_list:
    print(count, ': ', file.split('\\')[-1])
    count+=1


content = []  # location for the new data from the file to be loaded to then be parsed.
for file in file_list:
    try:
        with open(file) as f:
            content.extend(json.load(f))
    except:
        #Prints which file failed to open, as well as gives you a location to go test why the json load possibly failed.
        print ("Failed: " + file)
        print ("Looks like it is not a json format Go to https://jsonlint.com/ to fix the error")

#Displays the total number of objects to be processed.
print("Total entries: " + str(len(content)))

#starting of the parsing of the data
recorddata = []
for item in content:
    #print(item.keys())
    recorddata += (item["records"])
print(len(recorddata))    

#Would like to change this to a db store of raw data

#file_output = dt.now()
#file_output = file_output.strftime('%Y%m%d') + '_keycloak.json'

#Would like to add the ability to do some preloading of files vs having to reload the data every run
# sets the folder path to collect log data.
#sets the raw imports of properly formatted files.
file_list2 = glob.glob(os.path.join(path2, "*keycloak*.json"))

#same function as before:
print('List of imported files:')
count = 0
for file in file_list2:
    print(count, ': ', file.split('\\')[-1])
    count+=1

#Loading the files
#How do I take the info from the first run and load it without saving the file?
content2 = []
for data in file_list2:
    try:
        with open(data, encoding='utf-8') as f:
            content2.extend(json.load(f))
    except:
        #Prints which file failed to open, as well as gives you a location to go test why the json load possibly failed.
        print ("Failed: " + data)
        print ("Go to https://jsonlint.com/ to fix the error")

#Displays the total number of objects to be processed.
print("Total entries: " + str(len(content)))

#setting the data for processing.    
log_db = []
count = 0
tempDate = str
tempTime = str
logKeys = ['type', 'clientId', 'userId', 'username', 'identity_provider_identity']

idstore = []


for entry in content2:

    tempLog = {}

    try:

        if '@message' in entry:

            tempLog['id'] = entry['@message']['json']['time']

            logitem = entry['@message']['json']['log'].split(',')

            tempDate, tempTime = entry['@message']['json']['time'].split('T')

            tempLog['year'] = int(tempDate.split('-')[0]) 
            tempLog['month'] = int(tempDate.split('-')[1])
            tempLog['day'] = int(tempDate.split('-')[2])        
            tempLog['time'] = tempTime.split('Z')[0]

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
       # print(str(count) + '. Failed entry: ' + entry['@message'])
       pass
    count += 1

#reset count for the new data.
count = 0
for entry in content:

    tempLog = {}

    try:

        if '@message' in entry:

            tempLog['id'] = entry['@message']['json']['time']

            logitem = entry['@message']['json']['log'].split(',')

            tempDate, tempTime = entry['@message']['json']['time'].split('T')

            tempLog['year'] = int(tempDate.split('-')[0]) 
            tempLog['month'] = int(tempDate.split('-')[1])
            tempLog['day'] = int(tempDate.split('-')[2])        
            tempLog['time'] = tempTime.split('Z')[0]

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
       # print(str(count) + '. Failed entry: ' + entry['@message'])
       pass
    count += 1

#clean keys:

for item in log_db:
    for key in item:
        try:
            if '"' in item[key]:
                item[key] = item[key].strip('"')
                #print(item[key])
        except:
            pass

#clean up the open search data
for log in log_db:
    for key in log:
        if key == 'clientId':
            if 'https' in log[key]:
                log[key] = 'metadata'
                #print(log[key])

#Collect system user info
systemuserfiles = glob.glob(os.path.join(path2, "response*.json"))
systemuserList = []
# Fresh load of system user info, would want to update this for tracking system users but would like to see how I can track users that have been deleted from the system.
try:
    with open(systemuserfiles[0], "r") as f:
        systemuserfiles = json.load(f)
except:
    print('failed')

#Collect User Account Requests list:
userrequestfile = glob.glob(os.path.join(path2, "*.csv"))
userrequestList = []

try:
    count = 0
    with open(userrequestfile[0], 'r') as f:
        #input data clean up
        for line in f:
            line = line.strip('\n')
            line = line.rstrip('"')
            line = line.lstrip('"')
            #print(line)
            if count == 0:
                line = line.split('","')
                userKey = line
                #print(userKey)
                count += 1
            else:
                line = line.split('","')
                userrequestList.append(dict(zip(userKey, line)))
                #print(userrequestList[count-1])
                count+=1        
except:
    pass

#Pulls users out of the data from the system user list
userList2 = []
for n in systemuserList['data']['Users']:
    #print(n)
    userList2.append(n)

missingUser = ""
for user in userList2:
    orgFlag = False
    for org in userrequestList:
        if user['email'] == org['Email']:  #compares email addresses between system and acct req files.
            user['org'] = org['RK-Organization'] 
            orgFlag = True
    if orgFlag == False:
        #print(user['username'])
        txt = user['username']
        missingUser = missingUser + txt + ', '  #should provide a list of users on the system that are not in the acct req file
#clean up the list
missingUser = missingUser.rstrip(', ')
missingUser = missingUser.split(', ')
#Need to understand what I want to do with this list now.

#Used to track the users that are in the RK system user list but not the RKR Jira user list.  This will identify potentially the developers if done correctly.
count = 1

for user in missingUser:
    print(str(count)+ ': ' + user)
    count+=1

print(len(missingUser))

missingOrg = ''
txt = ''
for org in userrequestList:
    userFlag = False
    for user in userList2:
        if org['Email'] == user['email']:
            userFlag = True
            #print 
            pass
    if userFlag == False:
        txt = org['Email']
        missingOrg = missingOrg + txt + ', '

#This cleans up data of users that are in the Acct Req List vs the system user list
missingOrg = missingOrg.rstrip(', ')
missingOrg = missingOrg.split(', ')

#what do i do with this in the new build?
#used to track the users that are in the RKR jira list but not in the RK system user list
count = 1
for user in missingOrg:
    print(str(count) +': '+user)
    count+=1

#Tracks unique users and their id's to users to track
UserInfo = []
tempUser = {}

#UserInfo = [{'userId': 'f670759e-2af0-4114-977b-b3be9182e882', 'username': 'johnathon.murray.ctr'}]

for log in log_db:
    if 'userId' and 'username' in log.keys():
        tempUser = {'userId': log['userId'], 'username': log['username']}
    if tempUser in UserInfo:
        pass
    else:
        #print(tempUser)
        UserInfo.append(tempUser)

try:
    UserInfo.remove({})
except:
    pass

print(len(UserInfo))
#Provides the list of users on the system, would probably like to turn this into some other activity with the userId's as well, as I think this is why I have some duplicate users.
for user in UserInfo:
    print(user['username'])

#This helps with providing names to system userIds
count = 0
for log in log_db:
    if 'username' not in log.keys():
        for user in UserInfo:
            if user['userId'] == log['userId']:
                log_db[count]['username'] = user['username']
                
    count+=1

#Adding Org to the system logs from the Acct Req List Orgs id'd by users.
count = 0
for log in log_db:
    for user in userList2:
        if user["id"] == log['userId']:
            #print(user['Gov Organization'])
            log_db[count]['RK-Org'] = user['attributes']['gov-organization']
            if user['username'] not in missingUser:
                #sets the org to what has been defined as the users owning org per the Request for acount ticket.
                log_db[count]['org'] = user['org']
            else:
                #sets org to vendor that is developing/maintaining the system.  Backend user accounts.
                log_db[count]['org'] = 'BAH' 
    count +=1

#converts the data into a data frame:
df_loginfo = fu.DataFrame(log_db)

#would like to take the info in this file and build a web front to display the data that is now pushed to exce.
#Or I would like to update the below code to do more than just post new data to excel but also update dashboards in excel based on the data.
#Attempt to open the excel file if it fails give an error.
try:
    workbook = load_workbook(filename="Main_User_Info.xlsx")
except:
    print("didn't work")

#load the log sheet that is where the data will be updated.
sheet = workbook["logs"]

#This is to understand the range of the table that is current in the log sheet
sheet.tables.items()

workbook.remove(sheet)

print(workbook.sheetnames)

sheet = workbook.create_sheet('logs')

for row in dataframe_to_rows(df_loginfo, index=False, header=True):
    sheet.append(row)

#Find the size of the columns and rows after importing the new data.
maxColumn = sheet.max_column
maxRow = sheet.max_row

#list of letters 0 - 25
abc = list(string.ascii_lowercase)
tablerow = str(maxRow)
tablecolumn = maxColumn

if tablecolumn > 26 and tablecolumn < 650:
    extraL = (tablecolumn // 26) - 1
    while tablecolumn > 26:
        tablecolumn = tablecolumn - 26
tablecolumn = abc[tablecolumn-1]

#Sets the range for the table to be created based on the newly loaded data.
tablesize = 'A1:' + abc[maxColumn-1] + str(maxRow)
#Can output the range if needed for troubleshooting.

#Define the name of the table in the log sheet, and the size.
tab = Table(displayName="Table1", ref=tablesize)

#Creates a style to apply to the table when it is created.
style = TableStyleInfo(name="TableStyleMedium9", showFirstColumn=False, showLastColumn=False, showRowStripes=True, showColumnStripes=True)

#This pulls everything together and then creates the table.
tab.tableStyleInfo = style

sheet.add_table(tab)

#Creates the updated dashboard file to be used in the future.
workbook.save('test.xlsx')
