
#Create Python methods and MongoDB queries to insert new comments, movies, theatres, and users
# into respective MongoDB collections.


from pymongo import MongoClient

client = MongoClient()
db = client['Assignment']
comments = db['comments']

new_comment = {
    "movie_id": "tt0123456",
    "user_id": "user123",
    "text": "This movie was great!",
    "date": "2023-04-06"
}

result = comments.insert_one(new_comment)
print(result.inserted_id)


movies = db['movies']

new_movie = {
    "title": "The Shawshank Redemption",
    "year": 1994,
    "genres": ["Drama"],
    "cast": ["Tim Robbins", "Morgan Freeman"],
    "directors": ["Frank Darabont"],
    "writers": ["Stephen King"],
    "imdb": {
        "id": "tt0111161",
        "rating": 9.3,
        "votes": 2472266
    }
}

result = movies.insert_one(new_movie)
print(result.inserted_id)


theatres = db['theatres']

new_theatre = {
    "name": "AMC Dine-In Theatres",
    "location": {
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zipcode": "12345"
        },
        "geo": {
            "type": "Point",
            "coordinates": [-122.1234, 37.4321]
        }
    }
}

result = theatres.insert_one(new_theatre)
print(result.inserted_id)