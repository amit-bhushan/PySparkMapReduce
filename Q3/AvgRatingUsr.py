# Imports the PySpark libraries
from pyspark import SparkConf, SparkContext
from past.builtins.misc import xrange

# Configure the Spark context to give a name to the application
sparkConf = SparkConf().setAppName("AvgRatingUsr")
sc = SparkContext(conf = sparkConf)

def occupation_dictionary():
    occupation_dict={}
    with open("\\occupation.dat") as f:
        for each_occupation in f:
            fields=each_occupation.split("\t")
            occupation_dict[int(fields[0])]=fields[1].rstrip()
    return (occupation_dict)

occupation_bcast=sc.broadcast(occupation_dictionary())

rdd_ratings=sc.textFile("\\ratings.dat")

rdd_users=sc.textFile("\\users.dat")

rdd_movies=sc.textFile("\\movies.dat")

movies_ratings=rdd_ratings.map(lambda x:(int(x.split("::")[0]),(int(x.split("::")[1]),int(x.split("::")[2]))))

user_data=rdd_users.map(lambda x:(int(x.split("::")[0]),(int(x.split("::")[2]),int(x.split("::")[3]))))

movies_data=rdd_movies.map(lambda x:(int(x.split("::")[0]),x.split("::")[2])).flatMap(lambda x:[(x[0],z) for z in x[1].split("|")])

userratings_movies_data = movies_ratings.join(user_data).map(lambda x:(x[1][0][0],(x[0],x[1][0][1],x[1][1][0],x[1][1][1])))

ratings_user_genre = userratings_movies_data.join(movies_data).map(lambda x: ((x[1][0][3],('18-' if x[1][0][2] < 18 else '18-35' if (x[1][0][2] >= 18 and x[1][0][2] <= 35) else '36-50' if (x[1][0][2] >= 36 and x[1][0][2] <= 50) else '50+'),x[1][1]),(x[1][0][1],1))).filter(lambda x:x[0][1] != '18-').sortByKey()
 
ratings_user_genre_avg=ratings_user_genre.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).mapValues(lambda x:(round(x[0]/x[1],3))).sortBy(lambda x:(x[0][0],x[0][1],x[1]),False).sortBy(lambda x:(x[0][0],x[0][1])).map(lambda x:((occupation_bcast.value[x[0][0]],x[0][1]),[(x[0][2],x[1])])).reduceByKey(lambda x,y: x+y).sortByKey()

out_data = ratings_user_genre_avg.collect()

with open("\\result.dat", "w") as f: 
    for i in xrange(len(out_data)-1):
        f.write(str(out_data[i][0])+" => " +str(out_data[i][1][0:5])+'\n')
    f.close()

