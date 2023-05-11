from starbase import Connection

c = Connection("172.22.0.5", "8080")

movies = c.table('movies')

if (movies.exists()):
    print('Dropping existing movies table\n')
    movies.drop()
movies.create('genre')

movieFile = open("./ml-latest-small/movies.csv")


batch = movies.batch()



for line in movieFile:
    values = line.split(',')

    if (len(values) == 3):
        batch.update(values[0], {'genre': {values[-1]: values[1]}})

    else:
        batch.update(values[0], {'genre': {values[-1]: values[1]+values[2]}})
movieFile.close()


batch.commit(finalize=True)


print("testing \n")
print(movies.fetch("1"))

