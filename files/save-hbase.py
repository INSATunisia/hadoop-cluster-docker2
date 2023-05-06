import happybase

connection = happybase.Connection("localhost", "60000")

ratings = connection.table('ratings')


ratingFile = open('/root/ratings.csv', 'r')

#batch = ratings.batch()

for line in ratingFile:
    (userId, movieId, rating, timestamp) = line.split()
    ratings.put(b'userId', {b'rating': {b'movieId': b'rating'}})
   # batch.update(userId, {'rating': {movieId: rating}})

ratingFile.close()

#batch.commit(finalize=True)


print(ratings.row(b'1'))
