# Imports the PySpark libraries
from pyspark import SparkConf, SparkContext

# Configure the Spark context to give a name to the application
sparkConf = SparkConf().setAppName("TopTenViewed")
sc = SparkContext(conf = sparkConf)

def movies_dictionary():
    movies_dict={}
    with open("\\movies.dat") as f:
        for each_movie in f:
            fields=each_movie.split("::")
            movies_dict[int(fields[0])]=fields[1]
    return (movies_dict)

rdd=sc.textFile("\\ratings.dat")

movies_bcast=sc.broadcast(movies_dictionary())

movies_one=rdd.map(lambda x:(int(x.split("::")[1]),1))    

movies_count=movies_one.reduceByKey(lambda x,y: x+y)

flippa=movies_count.map(lambda movies_count:(movies_count[1],movies_count[0])).sortByKey(False)
 
flippa_names=flippa.map(lambda movies_count:(movies_count[0],movies_bcast.value[movies_count[1]]))

finale_result=flippa_names.take(10)
 
for i in finale_result:
    print(i)
