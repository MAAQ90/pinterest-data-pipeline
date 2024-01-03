import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

data_points=0

def run_infinite_post_data_loop():
    global data_points
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        invoke_url_pin = "https://3kk7wwcu78.execute-api.us-east-1.amazonaws.com/pinterest/streams/streaming-0a3c6c045333-pin/record"
        invoke_url_geo = "https://3kk7wwcu78.execute-api.us-east-1.amazonaws.com/pinterest/streams/streaming-0a3c6c045333-geo/record"
        invoke_url_user = "https://3kk7wwcu78.execute-api.us-east-1.amazonaws.com/pinterest/streams/streaming-0a3c6c045333-user/record"
        #invoke_url = "https://YourAPIInvokeURL/<YourDeploymentStage>/streams/<stream_name>/record"
        
        with engine.connect() as connection:
                     
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_payload = json.dumps({
                    "StreamName": "streaming-0a3c6c045333-pin",
                    "Data":
                        {
                        "index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]
                        },
                    "PartitionKey": "data-stream_pin"
                    
                })

                headers = {'Content-Type': 'application/json'}
                response_pin = requests.request("PUT", invoke_url_pin, headers=headers, data=pin_payload)
                print(response_pin.status_code)
                #print('\npin_data:\n', response_pin.text)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
                geo_payload = json.dumps({
                    "StreamName": "streaming-0a3c6c045333-geo",
                    "Data":
                        {     
                        "ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]
                        },
                    "PartitionKey": "data-stream_geo"
                    
                })
                
                headers = {'Content-Type': 'application/json'}
                response_geo = requests.request("PUT", invoke_url_geo, headers=headers, data=geo_payload)
                print(response_geo.status_code)
                #print('\ngeo_data:\n',response_geo.text)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result = dict(row._mapping)
                user_result['date_joined'] = user_result['date_joined'].isoformat()
                user_payload = json.dumps({
                    "StreamName": "streaming-0a3c6c045333-user",
                    "Data":
                        {
                        #Data should be send as pairs of column_name:value, with different columns separated by commas       
                        "ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]
                        },
                    "PartitionKey": "data-stream_user"
                    
                })
                
                headers = {'Content-Type': 'application/json'}
                response_user = requests.request("PUT", invoke_url_user, headers=headers, data=user_payload)
                print(response_user.status_code)
                #print('\nuser_data:\n', response_user.text)
        
        #data_points+=1        
        #print(f'data points processed:{data_points}')
            
            #print('/npin_result:')
            #print(pin_result)
            #print('/ngeo_result:')
            #print(geo_result)
            #print('/nuser_result:')
            #print(user_result)
        

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')

#run_infinite_post_data_loop()
    
    


