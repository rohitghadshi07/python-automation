import csv
import requests
import json
import time
url2 = "https://node.servify.in/coreapiv5/v5/ConsumerProduct/retailPlanSale/"

products = []
with open('ProductData.csv','rt') as csvfile:
   csvreader = csv.reader(csvfile,delimiter = ',')
   for row in csvreader:
       products.append(row)
       #print(row)
print("Products Loaded")
#print(products)

headers2 = {
   'content-type': "application/json",
   'cache-control': "no-cache",
   }


headers = {
   'content-type': "application/json",
   'authorization': "lkpeamakmapkmsakmsamsakwxiyxixix###lopimangrdgrdgrdgr####",
   'cache-control': "no-cache",
   #'App': "WebApp",
   #'Origin': "https://servify.in",
   #'Countrycode': "IN"
}

ctr = 0
with open('Bharti-Activation.csv','rt',encoding='ISO-8859-1') as csvfile:
   reader = csv.reader(csvfile, delimiter = ',')
   for row in reader:
    mob=row[4]
    if len(mob)==10 and mob.isdigit():
       print(row[0].replace("Ê",""))
       productID = row[8]
       print(productID)
       for p in products:
        if p[0] == productID:
          productID = p[0]
          productName=p[1]
          print(productName)
          brandID = int(p[2])
          pscID = int(p[3])
          print("Found")
          break
       EmailID = row[2].replace("Ê","")
        
       if row[2] == "":
        EmailID = "customer@servify.tech"
       if productID == None:
        print(product, " failed")
        continue
       payload ={
            "ProductSubCategoryID": int(pscID),
            "BrandID": brandID,
            "AmountPaid": int(row[7]),
            "PartnerServiceLocationID": int(row[5]),
            "app": "Offline Retail",
           #"PartnerServiceLocationID":None,
            "ThirdPartyID":"SUP98T",
           #"Serial Number": jsonresp["data"]["serialNumber"],
           #"SerialNo": jsonresp["data"]["serialNumber"],
            "Serial Number":row[0].replace("Ê",""),
            "SerialNo":row[0].replace("Ê",""),
            "ProductName": productName,
            "ProductID":productID,
            "PlanID": int(row[6]),
           #"PlanID":6,
            "FirstName": row[1].replace("Ê",""),
            "MobileNo": row[4].replace("Ê",""),
            "EmailID": EmailID,
            "CurrencyID": 1,
           "ProductPurchaseCost":int(row[10]),
           "DateOfPurchase":row[3],
           "DeviceDateOfPurchase": row[9]
            }
       print(payload)
       response = requests.request("POST", url2, data=json.dumps(payload), headers=headers, verify=False)
       print(response.text)
       finalres=json.loads(response.text.encode('utf-8'))
               #msg=finalres["status_message"]
               #print(msg)
               
       if finalres['success']:
        ctr = ctr+1
        #time.sleep(5)
        #print('5 seconds completed')
        #if ctr == 1:
        # break
        print(ctr," completed")
        
       else:
         f=open("error.txt","a")
         f.write(row[0])
              #f.write(response.text)
         print("ERRORRRRRRRRRRRRR----------->PLEASEEEEEEEEE CHECKKKKKKKKKKK")
         #time.sleep(2)
         #print('2 seconds completed')
         continue

    else:
      f=open("error.txt","a")
      f.write(row[0] +"\n")
      f.write("     (Please check the contact details)")
      #f.write(response.text)
      print(row[0])
      print("ERRORRRRRRRRRRRRR-------->Please check the contact details")
      continue