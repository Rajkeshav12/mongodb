import pymongo
from pprint import pprint
client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = client['Assignment']
theaters = db['theaters']


#1.1) Top 10 cities with the maximum number of theatres

def Find_10_cities_top_theaters():
     pipe = [
         {"$group": {"_id" : "$location.address.city" , "count":{"$sum":1}}},
         {"$sort": {"cnt": -1}},
         {"$limit": 10 },
         {"$project" :{"city" : "$location.address.city", "count":"$cnt"}}
     ]
     print('Top 10 cities with the maximum number of theatres are')
     pprint(list(theaters.aggregate(pipe)))

Find_10_cities_top_theaters()



#1.2) find top 10 theatres nearby given coordinates

def top10theatersNear(cod):
    theaters.create_index([("location.geo", "2dsphere")])
    print()
    print('top 10 theatres nearby given coordinates')
    print()
    pprint(list(theaters.find(
        {
            "location.geo": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": cod
                    }}
            }
        },{"location": "$location.geo.coordiantes", "type":"$location.geo.type"}).limit(10)))

top10theatersNear([-111.89966,33.430729])