# Imports the PySpark libraries
from pyspark import SparkConf, SparkContext

# Configure the Spark context to give a name to the application
sparkConf = SparkConf().setAppName("TopTwentyRated")
sc = SparkContext(conf = sparkConf)


def movies_dictionary():
    movies_dict={}
    with open("movies file path") as f:
        for each_movie in f:
            fields=each_movie.split("::")
            movies_dict[int(fields[0])]=fields[1]
    return (movies_dict)

rdd=sc.textFile("ratings file path")

movies_bcast=sc.broadcast(movies_dictionary())

movies_one=rdd.map(lambda x:(int(x.split("::")[1]),(int(x.split("::")[2]),1)))    

movies_count=movies_one.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).filter(lambda x:x[1][1] > 40).mapValues(lambda x:(round(x[0]/x[1],2),x[1])).sortBy(lambda x:x[1][0],False)

flippa_names=movies_count.map(lambda x:(movies_bcast.value[x[0]],x[1]))

finale_result=flippa_names.take(20)
 
for i in finale_result:
    print(i)