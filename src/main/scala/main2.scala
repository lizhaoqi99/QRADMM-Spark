import breeze.linalg._
import org.apache.spark.sql.SparkSession
import scala.Array.ofDim

object main2 {
  def main(args: Array[String]): Unit = {
    val bufferedSource = scala.io.Source.fromFile("data/smart_grid_stability_augmented.csv")

    val n = 60000
    val p = 12
    val Xy: DenseMatrix[Double] = DenseMatrix.zeros(n, p+2)
    Xy(::, 0) := DenseVector.ones[Double](n)

    var count = 0
    for(line <- bufferedSource.getLines) {
      if(count > 0) {  // ignore first row of the data
        val row = line.split(",").map(_.trim).dropRight(1).map(_.toDouble)
        var i = 1
        for(ele <- row) {
          Xy(count-1, i) = ele
          i += 1
        }
      }
      count += 1
    }
    bufferedSource.close()

    val spark = SparkSession.builder()
      .appName("test2")
      .master("local[10]")
      .config("spark.driver.memory","8g")
      .config("spark.driver.cores","10")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/Users/steve/Desktop/checkpoint")

    var arr = ofDim[Double](p+2, n)
    for(i <- 0 to p+1) {
      val temp = Xy(::, i).toArray
      arr(i) = temp
    }
    arr = arr.transpose

    val data = sc.parallelize(arr)

    var beta: Array[Double] = Array(0)
    spark.time(
      beta = QRADMM_Lasso.solve(data, 0.5, 100, 1, 5, M = 20)
    )
    beta.foreach(println)

    spark.stop()

  }
}
