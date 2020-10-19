import boto3 
import csv
import json

# ----------------- Upload Json file to elastic search ------------------------

key = 0
with open('Yelp_Restaurants.csv') as csvfile:
    reader = csv.reader(csvfile)
    for row in reader:
        head = {"index":{"_index":"restaurants","_type":"Restaurant", "_id": str(key)}}
        data = {"Business_id":str(row[0]), "cuisine": str(row[2])}
        key +=1
        print(data)
        with open('restaurants.json','a') as outfile:
            json.dump(head, outfile)
            outfile.write('\n')
            json.dump(data, outfile)
            outfile.write('\n')


# The following is the curl command to post the json file to elastic search
# curl 'https://search-yelpyelprestaurant-ujbkvdusitv4oijcy4okrpppgm.us-east-2.es.amazonaws.com/_cat/health?v'
# curl -XPOST https://search-yelpyelprestaurant-ujbkvdusitv4oijcy4okrpppgm.us-east-2.es.amazonaws.com/_bulk --data-binary @restaurants.json -H 'Content-Type: application/json'
