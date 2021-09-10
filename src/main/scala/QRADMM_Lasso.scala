import breeze.linalg.{DenseMatrix, DenseVector, max, pinv, sum}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

object QRADMM_Lasso {
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
  def solve(x: RDD[Array[Double]], y: RDD[Double], beta: DenseVector[Double], max_iter: Int = 1000, rho: Double, lambda: Double, e_abs: Double = 1E-5, e_rel: Double = 1E-5): Array[Double] = {

    val sc = x.sparkContext
    var old_beta = beta
    val p = x.first().length
    val n = x.count()
    var curr_iter = 1

    /* TODO: wrap everything (x,y,beta,...) into a RDD and utilize map()*/

    while(curr_iter <= max_iter) {
      old_beta = beta
      for (j <- Seq(1,p)) {
        if(j == 0) {
//          beta(0) = sum(y-x*beta)/n
        }
      }
    }
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
    val data: RDD[Array[Double]] = sc.parallelize(Array(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0)))
    val rdd = data.repartition(5)
    val p = rdd.first().length
    println(p)
    val n = rdd.count()
    println(n)
//    val n = rdd.map(row=>(row.slice(0,p-1), row(p-1))).mapPartitionsWithIndex((i,a) => {
//      val ar = a.toArray
//      val n: Int = ar.length
//      val A = DenseMatrix.zeros[Double](n, p-1)
//      val b = DenseVector.zeros[Double](n)
//      var j = 0
//      ar.foreach(elem =>{
//        A(j,::) := new DenseVector[Double](elem._1).t
//        b(j) = elem._2
//        j = j + 1
//      })
//
//      // A, b, x, u, r
//      Iterator((i, (pinv(A.t*A+DenseMatrix.eye[Double](p-1)), A.t*b, DenseVector.rand(p-1), DenseVector.rand(p-1), DenseVector.rand(p-1))))
//    }).persist()

    spark.stop()
  }
}
