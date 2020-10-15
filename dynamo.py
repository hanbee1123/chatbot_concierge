import boto3
import os
import csv  

boto3 = boto3.Session(profile_name = 'nyu')
dynamodb = boto3.resource(
    'dynamodb',
    region_name='us-east-1'
)



counter = 0
table = dynamodb.Table("YelpRestaurant")
with table.batch_writer() as batch:
    with open('Yelp_Restaurants.csv') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                batch.put_item(
                    Item = {
                        'Business_id' : row[0],
                        'Name':row[1],
                        'Cuisine':row[2],
                        'Rating':row[3],
                        'NumberOfReviews':row[4],
                        'Address':row[5],
                        'ZipCode':row[6],
                        'InsertedAtTimeStamp':row[10]
                    }
                )
            except:
                print(row)
                counter+=1
                print(counter)


# table = dynamodb.Table('YelpRestaurant')
# print(table.item_count)
