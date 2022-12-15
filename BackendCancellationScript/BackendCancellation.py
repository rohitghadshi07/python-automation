from pickle import TRUE
from unittest import result
import mysql.connector
import pandas as pd
import sshtunnel
import csv
import paramiko
from os.path import expanduser
import requests
import json
import time

url2 = "https://croma-api.servify.in/v1/Croma/bulkCallClosure"


headers2 = {
   'content-type': "application/json"
   }


def connect_db():
  home = expanduser('~')
  mypkey = paramiko.RSAKey.from_private_key_file(home + '/Documents/SSHKEY/id_rsa');
  ssh_host='ssh.servify.tech'
  ssh_port=22
  sql_hostname='10.40.4.164'
  sql_port=3306
  tunnel=sshtunnel.SSHTunnelForwarder((ssh_host,ssh_port),
                                  ssh_username='jumpuser',
                                  ssh_pkey=mypkey,
                                  remote_bind_address=(sql_hostname, sql_port),
                                  )

  tunnel.start()
  mydb=mysql.connector.connect(host='127.0.0.1',user="rohit.g",passwd='Password',database='servify',port=tunnel.local_bind_port)
       #mycursor=mydb.cursor()
  print(mydb)
  return (mydb,tunnel)


def checkPart(csrid):
  (conn,tunnel)=connect_db()
  mycursor=conn.cursor()
  try:
    query2 = "SELECT * FROM `part_transaction` WHERE `EntityID` = %s"
    mycursor.execute(query2,(csrid,))
  except:
    print('SQL Error!!')
  data2 = mycursor.fetchall()
  print(data2)
  if data2:
    return True
  else:
    return False


def changes(csrid):
  (conn,tunnel)=connect_db()
  mycursor=conn.cursor()
  try:
    insertcsrlog = "INSERT INTO `consumer_servicerequest_log` (`ConsumerServiceRequestID`,`Status`,`StatusCode`,`UserID`,`Remarks`,`RemarksCode`,`Reason`,`StatusDate`,`SubstatusID`,`ServiceLocationEngineerID`,`CreatedDate`,`UpdatedDate`,`SourceType`,`DestinationType`,`SourceID`,`DestinationID`,`Active`,`Archived`) VALUES ( %s,'Service cancel',NULL,0,'Tech support, closed from backend',NULL,NULL,CURRENT_TIMESTAMP,NULL,NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL,'',0,0,1,0);"
    updatecsr = "UPDATE `consumer_servicerequest` SET `Status` = 'Service cancel' WHERE `consumer_servicerequest`.`ConsumerServiceRequestID` = %s"
    updatecp = "UPDATE `consumer_product` SET `ConsumerServiceRequestID` = '0' WHERE `consumer_product`.`ConsumerServiceRequestID` = %s"
    mycursor.execute(insertcsrlog,(csrid,))
    mycursor.execute(updatecsr,(csrid,))
    mycursor.execute(updatecp,(csrid,))
  except:
    print('SQL Error!!')
  data3 = mycursor.fetchall()
  print(data3)



def execute():
       (conn,tunnel)=connect_db()
       print('Database connected!!!')
       mycursor=conn.cursor()

       ctr = 0

       with open('ExternalReference.csv','rt',encoding='utf-8-sig') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')

        query="SELECT `ExternalReferenceID`,`ConsumerServiceRequestID`,`ServiceTypeID`,`status` FROM `consumer_servicerequest` WHERE `ExternalReferenceID` = %s"
        
        for row in reader:
         csrid=row[0]
         #mycursor=mydb.cursor()
         print(csrid)
         print(query)
         mycursor.execute(query,(csrid,))
         data = mycursor.fetchall()
         print(data)
         print(data[0][3])


         if data[0][3] == "Service cancel":
           f=open("cancel.txt","a")
           f.write(data[0][0])

         elif data[0][3] == "Service completed":
           f=open("completed.txt","a")
           f.write(data[0][0])

         else:
           if checkPart(data[0][1]):
             print('Parts added.')
             f=open("PartsAdded.txt","a")
             f.write(data[0][0])

           else:
             print('Execution!!!')
             changes(data[0][1])
             ExternalReferenceID = data[0][0]
             if data[0][2] != '35':
               payload1 ={
                  "ExternalIDs":[ExternalReferenceID],
                  "CurrentStatusCode": "E0014",
                  "CurrentStatusText": "Onsite"
                }
               print(payload1)
               response = requests.request("POST", url2, data=json.dumps(payload1), headers=headers2)
               print(response.text)
               finalres=json.loads(response.text.encode('utf-8'))
               if finalres['success']:
                 ctr = ctr+1
                 print(ctr," completed")
                 f=open("success.txt","a")
                 f.write(data[0][0])

             if data[0][2] == '35':
               payload2 ={
                  "ExternalIDs":[ExternalReferenceID],
                  "CurrentStatusCode": "E0014",
                  "CurrentStatusText": "Refund"
                }
               print(payload2)
               response = requests.request("POST", url2, data=json.dumps(payload2), headers=headers2)
               print(response.text)
               finalres=json.loads(response.text.encode('utf-8'))
               if finalres['success']:
                 ctr = ctr+1
                 print(ctr," completed")
                 f=open("success.txt","a")
                 f.write(data[0][0])
        conn.close()
        tunnel.close()


if __name__ == '__main__':
    #connect_db()
    execute()
