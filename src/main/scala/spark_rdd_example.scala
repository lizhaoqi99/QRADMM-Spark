import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object spark_rdd_example extends App {
  val spark = SparkSession
    .builder
    .appName("spark_rdd")
    .config("spark.master", "local")
    .getOrCreate()
  val sc = spark.sparkContext

  // create Spark RDD from .parallelize
  val rdd_seq= sc.parallelize(Seq
  (("Java", 20000), ("Python", 100000), ("Scala", 3000)))
  rdd_seq.foreach(println)

  println()

  // create another RDD from existing RDD
  val rdd1_seq = rdd_seq.map(row=>(row._1,row._2+10000))
  rdd1_seq.foreach(println)

  println()
  /*-----------------------------------------------------------------------------*/
  val rdd:RDD[String] = sc.textFile("test.txt")
  println("initial partition count:" + rdd.getNumPartitions)

  // use repartition() to increase or decrease the partitions
  // a very expensive operation
  val reparRdd = rdd.repartition(4)
  println("re-partition count:" + reparRdd.getNumPartitions)

  // use coalesce() to only decrease the partitions
  // it is an optimized version of repartition()
  // rdd.coalesce(3)

  // collect() is used to retrieve all the elements of the RDD
  rdd.collect().foreach(println)

  // rdd flatMap transformation
  val rdd2 = rdd.flatMap(f=>f.split(" "))
  rdd2.foreach(f=>println(f))

  // create a Tuple by adding 1 to each word
  val rdd3:RDD[(String,Int)]= rdd2.map(m=>(m,1))
  rdd3.foreach(println)

  // filter transformation
  // filter all words starts with "a"
  val rdd4 = rdd3.filter(a=> a._1.startsWith("a"))
  rdd4.foreach(println)

  // reduceBy transformation
  val rdd5 = rdd3.reduceByKey(_ + _)
  rdd5.foreach(println)

  // swap word,count and sortByKey transformation
  // first convert RDD[(String,Int]) to RDD[(Int,String]) using map transformation
  // and apply sortByKey which ideally does sort on an integer value
  val rdd6 = rdd5.map(a=>(a._2,a._1)).sortByKey()
  println("Final Result")

  // Action - foreach
  rdd6.foreach(println)

  // Action - count
  println("Count : "+rdd6.count())

  // Action - first
  val firstRec = rdd6.first()
  println("First Record : "+firstRec._1 + ","+ firstRec._2)

  // Action - max
  val datMax = rdd6.max()
  println("Max Record : "+datMax._1 + ","+ datMax._2)

  // Action - reduce
  // reduce() aggregate action function is used to calculate min, max, and total of elements in a dataset
  val totalWordCount = rdd6.reduce((a,b) => (a._1+b._1,a._2))
  println("dataReduce Record : "+totalWordCount._1)

  // Action - take
  // return the first number of elements specified
  val data3 = rdd6.take(3)
  data3.foreach(f=>{
    println("data3 Key:"+ f._1 +", Value:"+f._2)
  })

  spark.stop()
}
