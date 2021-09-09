import breeze.linalg.{DenseVector, max}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object QRADMM {
  // p.32
  def soft_threshold(x: DenseVector[Double], lambda: Double, rho: Double): DenseVector[Double] = {
    val kappa = lambda / rho
    val zeros = DenseVector.zeros[Double](x.length)
    max(zeros, x - kappa) - max(zeros, -x - kappa)
  }

  /**
   *
   * @param x        design matrix
   * @param y        response vector
   * @param beta     initial value of beta
   * @param max_iter maximum number of iterations
   * @param rho      augmentation parameter
   * @param lambda   penalty parameter
   * @param e_abs    absolute tolerance stopping constant
   * @param e_rel    relative tolerance stopping constant
   * @return vector of coefficient estimates of the linear quantile regression model
   */
  def solve(x: RDD[Array[Double]], y: RDD[Double], beta: RDD[Double], max_iter: Int = 1000, rho: Double, lambda: Double, e_abs: Double = 1E-5, e_rel: Double = 1E-5): Array[Double] = {
    val sc = x.sparkContext
    val old_beta = beta
//    val r = y - x * beta

    Array(0.0)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QRADMM")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    //    val x: DenseVector[Double] = DenseVector(1, 2, 3, 4, 5)
    //    print(soft_threshold(x, 0.1, 1))
    val rdd: RDD[Array[Double]] = sc.parallelize(Seq(Array(1.0, 2.0, 3.0), Array(1.0, 2.0, 3.0)))
    print(rdd)

    spark.stop()
  }
}
