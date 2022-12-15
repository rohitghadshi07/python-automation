import mysql.connector
import pandas as pd
import sshtunnel
import csv
import paramiko
from os.path import expanduser


def connect_db():
  home = expanduser('~')
  mypkey = paramiko.RSAKey.from_private_key_file(home + '/Users/rohitghadshi/Documents/SSHKEY/id_rsa');
  ssh_host='ssh.servify.tech'
  ssh_port=22
  sql_hostname='maxscale.servify.internal'
  sql_port=3306
  tunnel=sshtunnel.SSHTunnelForwarder((ssh_host,ssh_port),
                                  ssh_username='dbuser',
                                  ssh_pkey=mypkey,
                                  remote_bind_address=(sql_hostname, sql_port),
                                  )

  tunnel.start()
  mydb=mysql.connector.connect(host='127.0.0.1',user="farooqui.r",passwd='WuRgML8quLFGbxNd',database='prod_servify',port=tunnel.local_bind_port)
       #mycursor=mydb.cursor()
  print(mydb)
  return (mydb,tunnel)

def execute():
       (conn,tunnel)=connect_db()
       mycursor=conn.cursor()
       ctr = 0
       with open('Ras.csv','rt',encoding='ISO-8859-1') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',')
        query="SELECT bi.BusinessInvoiceID,bi.InvoiceNo,bi.Entity,bi.EntityID,bi.StartDate,bi.EndDate,ijm.Message,bi.ErrorMessage,ijm.isManual,ijm.Status FROM invoice_job_meta AS ijm INNER JOIN business_invoice AS bi ON bi.BusinessInvoiceID = ijm.BusinessInvoiceID WHERE ijm.Status = 8 AND date(ijm.CreatedDate) >= %s AND date(ijm.CreatedDate) <= %s"
        for row in reader:
         FromDate=row[0]
         ToDate=row[1]
         values=(FromDate,ToDate)
         #mycursor=mydb.cursor()
         mycursor.execute(query,values)
         columns = [desc[0] for desc in mycursor.description]
         data = mycursor.fetchall()
         df = pd.DataFrame(list(data), columns=columns)

         writer = pd.ExcelWriter('Data1.xlsx')
         df.to_excel(writer)
         writer.save()
         conn.close()
         tunnel.close()


if __name__ == '__main__':
    #connect_db()
    execute()