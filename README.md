# mongodb
mongodb Assignment solution





# Comments collection

1.1) Created an aggregation pipeline to group the comments by user name, count the number of comments made by each user, sort the results in descending order by the count, and then limit the results to 10.

1.2) Created an aggregation pipeline to group the comments by movie ID, count the number of comments for each movie, sort the results in descending order by the count, and then limit the results to 10.

1.3) Created an aggregation pipeline to first project the year and month from the date field, then match the documents where the year matches the given year, group the comments by month and count the number of comments for each month, project the month and count fields and exclude the _id field, and finally sort the results by month in ascending order.


# Movies collection


1.1) Created an aggregation pipeline to first match the documents where the IMDB rating is not empty, then sort the documents by IMDB rating in descending order, limit the results to N documents, and project the _id, title, and imdb.rating fields.

1.2) Created an aggregation pipeline to first match the documents where the IMDB rating is not empty and the year matches the provided year, then sort the documents by IMDB rating in descending order, limit the results to N documents, and project the _id, title, imdb.rating, and year fields.

1.3) It uses MongoDB's aggregation pipeline to filter and sort the movies based on the given criteria.

The pipeline consists of four stage 
$match stage filters the movies that have a non-empty imdb.rating, a year of 2002, and have more than or equal to 1000 votes in imdb.votes.
$sort stage sorts the filtered movies in descending order of imdb.rating.
$limit stage limits the output to n documents.
$project stage reshapes the documents to include only the _id, title, imdb.rating, and imdb.votes fields.

1.4) This function takes two arguments: n (the number of movies to display) and pattern (the regular expression pattern to match against movie titles). The $regex operator is used to perform the pattern matching, and the $options parameter is set to "i" to make the search case-insensitive.


2.1) The pipeline for the aggregation query first filters out movies where the directors field is None. Then, it groups the movies by their directors field, and counts the number of movies for each director using the $sum operator. The results are sorted in descending order based on the count using the $sort operator. The pipeline is then limited to return only the top n results using the $limit operator.


2.2) created an aggregation pipeline The pipeline consists of the following stages:
Match the documents where the year matches the given year.
Unwind the directors array to create a separate document for each director.
Group the documents by director and count the number of movies created by each director.
Sort the documents in descending order of the movie count.
Limit the number of documents to n.
Project the count and year for each document.


2.3) The pipeline consists of several stages. Unwinds the genres array so that each document represents one genre. Then match the documents that have the specified genre. group the documents by director and count the number of movies each director has made. sort the results in descending order based on the count field. Finally, project only the count field to be displayed in the output.


3.1) Created an aggregation pipeline to unwind the "cast" field (which contains a list of actors) and group by actor name to count the number of movies they have appeared in. sorted the result in descending order of the count and returned the top n actors. 

3.2) Created an aggregation pipeline that uses  
1) the $match stage to filter the movies based on the year. 
2) the $unwind stage to create a separate document for each cast member of the movie. 
3) groups the documents by cast member
4) the $sum operator to count the number of movies each actor has starred in. 
5) sorts the actors based on the count in descending order

3.3) used an aggregation pipeline to first match the documents that have the given genre, then unwind the cast array, group by the cast field to count the number of movies each actor has starred in, sort the actors in descending order based on the number of movies they have starred in, and finally limit the results to n

4) uses a pipeline that matches movies with that genre, sorts them in descending order based on IMDB rating, removes movies without a rating, projects the movie title and rating fields, and limits the results to the top N. Then prints the results for each genre.


# Therater collection


1.1) Uses a pipeline to first group the theaters collection by city and counts the number of theaters in each city using the $sum aggregation operator. It then sorts the cities in descending order based on the count of theaters and limits the result to the top 10 cities using the $limit aggregation operator. Finally, projects the city and count fields and prints the result 

1.2) defined a function top10theatersNear which takes a list of 2 float values representing the longitude and latitude of a location. The function creates a 2dsphere index on the location.geo field of the theaters collection, and then queries the collection to find the 10 theaters nearest to the given coordinates. The function prints the result
