import breeze.linalg._
import breeze.numerics.abs
import breeze.stats._
import breeze.stats.distributions.{Gaussian, MultivariateGaussian, Rand}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.Array.ofDim

object main {
  def main(args: Array[String]): Unit = {
    val n = 50000
    val p = 30
    val X: DenseMatrix[Double] = DenseMatrix.zeros[Double](n, p+1)
    X(::, 0) := DenseVector.ones[Double](n)
    for(i <- 1 to p) {
      X(::, i) := DenseVector.rand[Double](n)
    }
    val err = DenseVector.rand(n, Gaussian(0,1))
    val y = X(::, 0)*2.0 + X(::, 2)*1.5 + X(::, 3)*3.0 + err + 1.8

    var arr = ofDim[Double](p+2, n)
    for(i <- 0 to p) {
      val temp = X(::, i).toArray
      arr(i) = temp
    }
    arr(p+1) = y.toArray
    arr = arr.transpose

//    arr.foreach(x => println(x.mkString(", ")))
//    Rand.generator.setSeed(1)
//    val x1: DenseVector[Double] = DenseVector.rand(n)
//    Rand.generator.setSeed(2)
//    val x2: DenseVector[Double] = DenseVector.rand(n)
//    Rand.generator.setSeed(3)
//    val x3: DenseVector[Double] = DenseVector.rand(n)
//    Rand.generator.setSeed(4)
//    val err: DenseVector[Double] = DenseVector.rand(n, Gaussian(0,1))
//    val y: DenseVector[Double] = x1 * 0.5 + x2 * 2.0 + x3 * 2.0 + err + 2.0
//    val ones = DenseVector.ones[Double](n)
//    //    val xy = DenseVector.horzcat(DenseVector.ones[Double](100), x1, x2, y)
//
//    val x1_arr = x1.toArray
//    val x2_arr = x2.toArray
//    val x3_arr = x3.toArray
//    val y_arr = y.toArray
//    val ones_arr = ones.toArray
//    val xy: Array[Array[Double]] = Array(ones_arr, x1_arr, x2_arr, x3_arr, y_arr).transpose
//    xy.foreach(x => println(x.mkString(", ")))
//

    val spark = SparkSession.builder()
      .appName("test1")
      .master("local[10]")
      .config("spark.driver.memory","8g")
      .config("spark.driver.cores","10")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("checkpoint")

    val data = sc.parallelize(arr)

//    val t1 = System.nanoTime()
    var beta: Array[Double] = Array(0)
    spark.time(
      beta = QRADMM_Lasso.solve(data, 0.9, 100, 1, 20, M = 20)
    )
//    val beta = QRADMM_Lasso.solve(data, 0.5, max_iter = 50, 1, 1, M = 5)
//    val t2 = System.nanoTime()
//    val diff = (t2-t1)/1e9d
//
//    println(s"The program took $diff seconds.")
    beta.foreach(println)

    spark.stop()

  }
}
