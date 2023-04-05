import pymongo
from pprint import pprint
client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
db = client['Assignment']
movies = db['movies']


# 1.1) Print the title and IMDB rating for each movie of top N movies

def Find_top_N_movies_with_the_highest_IMDB_rating(n):
    pipe = [
        {"$match": {"imdb.rating": {"$ne": ""}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project" : {"id" : "$_id","title" : "$title" , "imdb-rating" : "$imdb.rating"}}
    ]
    print(f'The title and IMDB rating for each movie of top {n} movies')
    print()
    pprint(list(movies.aggregate(pipe)))


Find_top_N_movies_with_the_highest_IMDB_rating(10)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

# 1.2) top n with the highest IMDB rating in a given year

def find_top_n_movies_with_highest_imdb_rating_in_year(n,year):
    pipe = [
        {"$match": {"$and" : [{"imdb.rating": {"$ne": ""}},{"year" : 2002 }]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project": {"id": "$_id", "title": "$title", "imdb-rating": "$imdb.rating","year" : "$year" }}

    ]

    print(f'top {n} with the highest IMDB rating in a year {year}')
    print()

    pprint(list(movies.aggregate(pipe)))

find_top_n_movies_with_highest_imdb_rating_in_year(3,2002)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#1.3) top n movies with highest IMDB rating with number of votes > 1000

def find_top_n_movies_with_highest_imdb_rating_in_votes_greater_than_1000(n):
    pipe = [
        {"$match": {"$and" : [{"imdb.rating": {"$ne": ""}},{"year" : 2002 },{"imdb.votes" :{"$gte" : 1000 }}  ]}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n },
        {"$project": {"id": "$_id", "title": "$title", "imdb-rating": "$imdb.rating","imdb-votes": "$imdb.votes" }}

    ]

    print(f'top {n} movies with highest IMDB rating with number of votes > 1000')
    print()
    pprint(list(movies.aggregate(pipe)))

find_top_n_movies_with_highest_imdb_rating_in_votes_greater_than_1000(3)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

# 1.4) find top n movies with title matching a given pattern sorted by highest tomatoes ratings

def find_top_n_movies_with_highest_tomato_rating_and_pattern_matching(n, pattern):
    pipe = [
        {"$match": {"title": {"$regex": pattern, "$options": "i"}, "tomatoes.viewer.rating": {"$ne": ""}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": n},
        {"$project": {"id": "$_id", "title": "$title", "tomatoes-rating": "$tomatoes.viewer.rating"}}
    ]

    print(f'top {n} movies with title matching "{pattern}" sorted by highest tomatoes ratings')
    print()
    pprint(list(movies.aggregate(pipe)))


find_top_n_movies_with_highest_tomato_rating_and_pattern_matching(5, "lord")


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#2.1) find top n directors who created the maximum number of movies

def find_top_n_directors(n):
    pipeline = [
        {"$match": {"directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": pymongo.DESCENDING}},
        {"$limit": n},
        {"$project": {"_id": 0, "director": "$_id", "count": 1}}
    ]
    print(f'Top {n} directors who created the maximum number of movies:')
    pprint(list(movies.aggregate(pipeline)))


find_top_n_directors(10)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#2.2 find top n directors who created the maximum number of movies in a given year

def find_top_N_directors_in_year(n,year):
    pipeline2 = [

        {"$match": {"year" : year}},
        {"$unwind": "$directors"},
        {
            "$group": {
                "_id": "$directors",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": n
        },
        {"$project": {"count": "$count","year" : "$year"}}

    ]
    print(f'find top {n} directors who created the maximum number of movies in year {year}')
    pprint(list(movies.aggregate(pipeline2)))

find_top_N_directors_in_year(5, 2002)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#2.3) find top n directors who created the maximum number of movies for a given genre

def find_top_N_directors_in_genre(n,genre):
    pipeline2 = [
        {"$unwind": "$genres"},
        {"$match": {"genres" : genre}},
        {
            "$group": {
                "_id": "$directors",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": pymongo.DESCENDING
            }
        },
        {
            "$limit": n
        },
        {"$project": {"count": "$count"}}

    ]
    print(f'top {n} directors who created the maximum number of movies for a given genre {genre}')
    pprint(list(movies.aggregate(pipeline2)))

find_top_N_directors_in_genre(5,"Crime")

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")


#3.1) find top n actors who started in the maximum number of movies

def top_n_actors_with_max_movies(n):
    pipe = [
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
    ]
    print(f'top {n} actors who started in the maximum number of movies')
    print()
    pprint(list(movies.aggregate(pipe)))


top_n_actors_with_max_movies(10)

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#3.2) find top n actors who starred in the maximum number of movies in a given year

def top_n_actors_with_max_movies_in_year(n,year):
    pipe = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
    ]
    print('top n actors who starred in the maximum number of movies in a given year')
    print()
    pprint(list(movies.aggregate(pipe)))


top_n_actors_with_max_movies_in_year(10,2002)


print()
print()
print("-------------------------------------------------------------------------------------------------------------------")

#3.3) Finf top n actors who starred in the maximum number of movies for a given genre

def top_n_actors_with_max_movies_in_year(n,genre):
    pipe = [
        {"$unwind": "$genres"},
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group" : {"_id" : "$cast","count" : {"$sum" : 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n },
        {"$project" : {"genre" : "$genres","count" : "$count"}}
    ]
    print(f'top {n} actors who starred in the maximum number of movies for a given genre {genre}')
    print()
    pprint(list(movies.aggregate(pipe)))


top_n_actors_with_max_movies_in_year(10,"Crime")

print()
print()
print("-------------------------------------------------------------------------------------------------------------------")


#4) Find top `N` movies for each genre with the highest IMDB rating

def topNMoviesForAGenre(N):
    pipe=[
        {"$unwind":"$genres"},
        {"$group":{"_id":"$genres"}}
    ]
    print(f'Top {N} movies for each genre with the highest IMDB rating')
    for i in list(movies.aggregate(pipe)):
        genre=i['_id']
        print("Genre: "+genre)
        pipe=[
            {"$match":{"genres":genre}},
            {"$sort":{"imdb.rating":-1}},
            {"$match":{"imdb.rating":{"$ne":""}}},
            {"$project":{"_id":0,"title":1,"rating":"$imdb.rating"}},
            {"$limit":N}
        ]
        pprint(list(movies.aggregate(pipe)))

topNMoviesForAGenre(4)

