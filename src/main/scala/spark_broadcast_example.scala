import org.apache.spark.sql.SparkSession

object spark_broadcast_example extends App {
  val spark = SparkSession.builder()
    .appName("spark_broadcast")
    .master("local")
    .getOrCreate()

  val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
  val countries = Map(("USA","United States of America"),("IN","India"))

  val broadcastStates = spark.sparkContext.broadcast(states) // keep a copy of states on each node
  val broadcastCountries = spark.sparkContext.broadcast(countries)

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val rdd = spark.sparkContext.parallelize(data) // use parallelize to create spark RDD

  val rdd2 = rdd.map(f=>{
    val country = f._3
    val state = f._4
    val fullCountry = broadcastCountries.value(country)
    val fullState = broadcastStates.value(state)
    (f._1,f._2,fullCountry,fullState)
  })

  println(rdd2.collect().mkString("\n"))

  spark.stop()
}
