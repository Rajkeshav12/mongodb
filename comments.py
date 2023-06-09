import pymongo
client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = client['Assignment']
comments = db['comments']



#1 Find top 10 users who made the maximum number of comments

def top_10_users():
    pipeline = [
        {
            "$group": {
                "_id": "$name",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": 10
        }
    ]
    results = comments.aggregate(pipeline)
    print('Top 10 users with maximum number of comments are')
    print()
    for doc in results:
        print(doc)

top_10_users()

print()
print()
print("-----------------------------------------------------------------------------------------------------------")

#2 Find top 10 movies with most comments

def top_10_with_comments():
    pipeline2 = [
        {
            "$group": {
                "_id": "$movie_id",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": 10
        }
    ]
    print('Top 10 movies with most comments are')
    print()
    results2 = comments.aggregate(pipeline2)
    for doc in results2:
        print(doc)


top_10_with_comments()

print()
print()
print("-----------------------------------------------------------------------------------------------------------")

#3
#Given a year find the total number of comments created each month in that year
# Define the pipeline stages as a list of dictionaries

def monthWiseComment(year):
    pipeline3 = [
        {"$project": {"year": {"$year": "$date"}, "month": {"$month": "$date"}}},
        {"$match": {"year": year}},
        {"$group": {"_id": "$month", "count": {"$sum": 1}}},
        {"$project": {"month": "$_id", "count": 1, "_id": 0}},
        {"$sort": {"month": 1}}
    ]

    results2 = comments.aggregate(pipeline3)
    print(f'find the total number of comments created each month in that year {year}')
    print()
    for doc in results2:
        print(doc)

monthWiseComment(2015)