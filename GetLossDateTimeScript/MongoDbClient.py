import os
from nbformat import write
import csv
import pymongo
import paramiko
import sshtunnel


class MongoDbClient:
    def __init__(self, mongo_config, ssh_config, user_creds, with_sshtunnel):
        self.logger = None
        self.mongo_config = mongo_config
        self.user_creds = user_creds
        self.ssh_config = ssh_config
        self.with_sshtunnel = with_sshtunnel

    def __enter__(self):
        if self.with_sshtunnel == 'yes':
            key = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~') + self.ssh_config['key_path'])
            self.tunnel = sshtunnel.SSHTunnelForwarder((self.ssh_config['hostname'], int(self.ssh_config['port'])),
                                                    ssh_username = self.ssh_config['username'],
                                                    ssh_pkey = key,
                                    remote_bind_address = (self.mongo_config['host_url'], int(self.mongo_config['port']))
            )
            self.tunnel.start()

            self.client = pymongo.MongoClient(host = '127.0.0.1',
                                     port = self.tunnel.local_bind_port,
                                     username = self.mongo_config['username'],
                                     password = self.user_creds['mongo_password'])
            self.db = self.client[self.mongo_config['db_name']]
            print("Uri is :",self.client)
            #print("Connected to the mongo db India")
        else:
            user_name = self.mongo_config['username']
            password = self.user_creds['mongo_password']
            url = self.mongo_config['host_url']
            db_name = self.mongo_config['db_name']
            uri = "mongodb://{}:{}@{}/".format(user_name, password, url)
            client = pymongo.MongoClient(uri)
            self.db = client[db_name]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def select_record(self, user_id):
        collection = self.db["userAccessConfig"]
        json = {"userID": user_id}
        id_val = {"_id": 0}
        response = collection.find_one(json, id_val, max_time_ms=60000)
        return response

    def select_record_LossDateTime(self, ConsumerServiceRequestID):
        collection = self.db["consumerServiceRequestDetail"]
        json = {"ConsumerServiceRequestID": ConsumerServiceRequestID}
        id_val = {"_id": 0,"ConsumerServiceRequestID":1,'lossDateTime':1}
        response = collection.find_one(json, id_val, max_time_ms=60000)
        return response

    def check_feature_rights(self, user_id,feature_rights):
        collection = self.db["userAccessConfig"]
        json = {"userID": user_id,"FeatureRights": feature_rights}
        id_val = {"_id": 0,"userID":1,'FeatureRights':1}
        response = collection.find_one(json, id_val, max_time_ms=60000)
        return response

    def update_feature_rights(self, user_id, feature_rights):
        collection = self.db["userAccessConfig"]
        json = {"userID": user_id}
        new_json = { "$push": { "FeatureRights": [feature_rights]}}
        response = collection.update(json,new_json)
        return response.modified_count

    def insert_record(self, details):
        collection = self.db["userAccessConfig"]
        response = collection.insert_one(details)
        self.print_log("Mongo insert response {} ".format(response))
        return response

    def print_log(self, message):
        if self.logger is not None:
            self.logger.info(message)
        else:
            print(message)

    def update_record(self, user_id):
        collection = self.db["userAccessConfig"]
        json = {"userID" : user_id}
        new_json = { "$set": { "isActive": False}}
        response = collection.update_one(json,new_json)
        return response.modified_count
