from re import L, X
from MongoDbClient import MongoDbClient
#from nbformat import write
import csv
import xlsxwriter
#import pandas as pd

#Connection
mongo_config = {"host_url":"mongo.servify.internal","port":"27017","username":"krishna.t","db_name":"ProdApiAuth"}
ssh_config = {"key_path":"/Documents/SSHKEY/id_rsa","hostname":"ssh.servify.tech","port":"22","username":"jumpuser"}
user_creds = {"mongo_password":"sEqxCykba7dxrLmc"}
with_sshtunnel = "yes"
print(mongo_config["db_name"])


with MongoDbClient(mongo_config, ssh_config, user_creds, with_sshtunnel) as mongo:
    result = mongo.select_record_LossDateTime(21486)
    result["lossDateTime"] = result["lossDateTime"].strftime("%Y-%m-%d %H:%M:%S")
    print(result)


    def execution(mongoDB):
        query_list = []
        with open("CSR.csv","rt",encoding="ISO-8859-1") as csvfile:
            reader = csv.reader(csvfile, delimiter = ",")
            for row in reader:
                ConsumerServiceRequestID = row[0]
                ConsumerServiceRequestID = int(ConsumerServiceRequestID)
                query = mongoDB.select_record_LossDateTime(ConsumerServiceRequestID) 
                print("Query is ", query)          
                query["lossDateTime"] = query["lossDateTime"].strftime("%Y-%m-%d %H:%M:%S")
                print("Query is ", query)
                query_list.append(query)
                
        #Generate excel sheet and content addition       
        workbook = xlsxwriter.Workbook("LossDateTime.xlsx")
        worksheet = workbook.add_worksheet("My sheet")
        row1 = 0
        headers = ["LossDateTime","ConsumerServiceRequestID"]
        for header in range(len(headers)):
            worksheet.write(row1,header,headers[header])
            print(row1,header,headers[header])
        row1 = 1
        print("querriiiieesss:", query_list)
        for i in query_list:
            col = 0
            for key in i:
                worksheet.write(row1, col, i[key])
                print("values are:", (row1,col,i[key]))
                col += 1
            row1 += 1
        workbook.close()
                

if __name__ == "__main__":
    #connect_db()
    with MongoDbClient(mongo_config, ssh_config, user_creds, with_sshtunnel) as mongoDB:
        execution(mongoDB)