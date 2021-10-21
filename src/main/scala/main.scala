import breeze.linalg._
import breeze.stats._
import breeze.stats.distributions.{Gaussian, Rand}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    Rand.generator.setSeed(10)
    val x1: DenseVector[Double] = DenseVector.rand(20)
    Rand.generator.setSeed(10)
    val x2: DenseVector[Double] = DenseVector.rand(20)
    Rand.generator.setSeed(10)
    val err: DenseVector[Double] = DenseVector.rand(20, Gaussian(0,1))
    val y: DenseVector[Double] = x1 * 3.5 + x2 * 5.0 + err + 2.0
    val ones = DenseVector.ones[Double](20)
    //    val xy = DenseVector.horzcat(DenseVector.ones[Double](100), x1, x2, y)

    val x1_arr = x1.toArray
    val x2_arr = x2.toArray
    val y_arr = y.toArray
    val ones_arr = ones.toArray
    val xy: Array[Array[Double]] = Array(ones_arr, x1_arr, x2_arr, y_arr).transpose

    val spark = SparkSession.builder()
      .appName("main")
      .master("local")
      .config("spark.driver.memory","3g")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("/Users/steve/Desktop/checkpoint")

    val data = sc.parallelize(xy)

    val beta = QRADMM_Lasso.solve(data, 0.5, max_iter = 100, 1, 1, M = 5)
    beta.foreach(println)

    spark.stop()
  }
}
