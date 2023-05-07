from starbase import Connection

c = Connection("172.22.0.5", "8080")

ratings = c.table('ratings')

if (ratings.exists()):
    print('Dropping existing ratings table\n')
    ratings.drop()
ratings.create('rating')

ratingFile = open("./merged_data.csv")




batch = ratings.batch()

#movieId,title,genre,userId,rating,timestamp

for line in ratingFile:
    values = line.split(',')
    if (len(values)==6):
        batch.update(values[3], {'rating': {values[1]: values[4]}}, timestamp=values[5])
    else:
        # batch.update(values[0], {'infos': {values[-4]: values[1]+values[2]},'rating': {values[-3]: values[-2]}}, timestamp=values[-1])
        continue

ratingFile.close()


batch.commit(finalize=True)


print("testing \n")
print(ratings.fetch("1"))

