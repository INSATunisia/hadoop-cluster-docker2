from starbase import Connection

c = Connection("172.22.0.5", "8080")

ratings = c.table('ratings')

if (ratings.exists()):
    print('Dropping existing ratings table\n')
    ratings.drop()
ratings.create('rating')

ratingFile = open("./ml-latest-small/ratings.csv")




batch = ratings.batch()


#userId,movieId,rating,timestamp
for line in ratingFile:
    values = line.split(',')
    if (len(values)==4):
        batch.update(values[0], {'rating': {values[1]: values[2]}}, timestamp=values[3])
    else:
        # batch.update(values[0], {'infos': {values[-4]: values[1]+values[2]},'rating': {values[-3]: values[-2]}}, timestamp=values[-1])
        continue

ratingFile.close()


batch.commit(finalize=True)


print("testing \n")
print(ratings.fetch("1"))

