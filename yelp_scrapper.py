import json
import requests
import boto3
from datetime import datetime
from decimal import Decimal
import csv

API_KEY= 'LpTJn44WKLXQ3cp4RqiQz323tWXNy-krqgw7FFNjd0coIb8v-FhlLds-Ei4l6K_1oe_sGK904F7-hSk4oN-EeON4E0xIH3vThY3_-9MnMLs-RxCEyNGF8oSJNiyIX3Yx'
CLIENT_ID = 'N7n5qG_rX5UWwdtsTZZKkw'

API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.
YELP_ENDPOINT = 'https://api.yelp.com/v3/businesses/search'

CSV_HEAD = ['id', 'Name', 'Cuisine', 'Rating', 'NumberOfReviews',
            'Address', 'ZipCode', 'Latitude', 'Longitude', 'IsClosed',
            'InsertTime']
CSV_FILE = 'Yelp_Restaurants.csv'

CUISINES =['indian', 'mexican', 'japanese','chinese', 'coffee']
LIMIT_DEFAULT = 50

def valid(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input

# write to CSV file
def writeCSV(data):
    with open(CSV_FILE, 'a+', newline='', encoding='utf-8') as f:
        f_csv = csv.DictWriter(f, CSV_HEAD)
        f_csv.writeheader()
        f_csv.writerows(data)

def scrape_yelp():
    payload = {
        "location": 'new york city',
        "limit":LIMIT_DEFAULT,
        "radius":40000
    }
    headers = {
        'Authorization': 'Bearer %s' % API_KEY,
    }
    seen = []
    for cuisine in CUISINES:
        business_count = 0
        current_load = []
        csv_data_file = []
        payload['categories'] = cuisine
        payload["offset"] = 0
        print(payload)
        #Scrape data from yelp
        
        while business_count < 1000:
            if business_count + 50 > 1000:
                break
            payload["offset"] = business_count
            response = requests.get(YELP_ENDPOINT, params=payload, headers=headers)
            response_data = response.json()
            
            # Check for restaurant duplication
            for data in response_data['businesses']:
                if data not in seen:
                    seen.append(data)
                    current_load.append(data)
                    business_count += 1

            print(business_count)
        for csv_data in current_load:
            time_string = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            item = {
                        CSV_HEAD[0]: valid(csv_data['id']),
                        CSV_HEAD[1]: valid(csv_data['name']),
                        CSV_HEAD[2]: valid(cuisine),
                        CSV_HEAD[3]: valid(Decimal(csv_data['rating'])),
                        CSV_HEAD[4]: valid(Decimal(csv_data['review_count'])),
                        CSV_HEAD[5]: valid(csv_data['location']['address1']),
                        CSV_HEAD[6]: valid(csv_data['location']['zip_code']),
                        CSV_HEAD[7]: valid(str(csv_data['coordinates']['latitude'])),
                        CSV_HEAD[8]: valid(str(csv_data['coordinates']['longitude'])),
                        CSV_HEAD[9]: valid(str(csv_data['is_closed'])),
                        CSV_HEAD[10]: valid(time_string)
                    }            
            csv_data_file.append(item)
        writeCSV(csv_data_file)

def upload_dynamodb():
    # Connect to dynamodb
    boto3 = boto3.Session(profile_name = 'nyu')
    dynamodb = boto3.resource(
        'dynamodb',
        region_name='us-east-1'
    )
    # Load csv file to dynamo db
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

if __name__ == "__main__":
    scrape_yelp()
    upload_dynamodb()
