import breeze.linalg._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

object QRADMM_Lasso {

// p.32
  def soft_threshold(x: DenseVector[Double], y: DenseVector[Double]): DenseVector[Double] = {
    val zeros = DenseVector.zeros[Double](x.length)
    max(zeros, x - y) - max(zeros, -x - y)
  }

  /**
   *
   * Assume the intercept is included in the model so that the first column of the design matrix x is a vector of 1
   * @param x_y      a rdd collection consists of the design matrix and the response y
   * @param tau      quantile of interest
   * @param max_iter maximum number of iterations
   * @param rho      augmentation parameter
   * @param lambda   penalty parameter
   * @param ABSTOL   absolute tolerance stopping constant
   * @param RELTOL   relative tolerance stopping constant
   * @param M        number of partitions of the original data
   * @return vector of coefficient estimates of the linear quantile regression model
   */
  def betaUpdate_lasso(x_y: RDD[Array[Double]], tau: Double, max_iter: Int = 1000, rho: Double, lambda: Double, ABSTOL: Double = 1E-7, RELTOL: Double = 1E-4, M: Int): Array[Double] = {

    val sc = x_y.sparkContext
    val p = x_y.first().length - 2  // assume x includes leading 1's
    val n = x_y.count()

    val ni = n/M
    val lambda_adjusted = lambda/n
    val rho_adjusted = rho/n
    val data = x_y.repartition(M)

    // calculation will be done by partition/block
    // D: RDD[(partition index, (x_b, y_b, beta_b, r_b, u_b, eta_b))]
    // length of D is M
    val D: RDD[(Int, (DenseMatrix[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double]))] = {
      data.mapPartitionsWithIndex((i, x_y_block) => {
        val x_y_b = DenseMatrix(x_y_block.toArray: _*) // https://stackoverflow.com/questions/48166561/create-a-breeze-densematrix-from-a-list-of-double-arrays-in-scala
        val x_b = x_y_b(::, 0 to p)
        val y_b = x_y_b(::, p + 1)
        Iterator((i, (x_b, y_b, DenseVector[Double](p + 1), DenseVector[Double](n / M), DenseVector[Double](n / M), DenseVector[Double](p + 1))))
      }).persist()
    }

    // TODO: update checkpoint directory
    D.checkpoint()  // break RDD lineage at each iteration

//    val r = y-xmat*beta
    var beta = DenseVector.zeros[Double](p + 1)
    var betaold = beta

    breakable{
      for (curr_iter <- 1 to max_iter) {

        val beta_avg = D.map(a => a._2._3).reduce(_+_)/M.toDouble
        val eta_avg = D.map(a => a._2._6).reduce(_+_)/M.toDouble
        betaold = beta

        // update beta
        val df = DenseVector.fill(p+1, lambda_adjusted)
        val beta_0 = beta_avg(0) + eta_avg(0)/rho_adjusted
        val beta_1_to_p = soft_threshold(beta_avg.slice(1,p+1)+eta_avg.slice(1,p+1)/rho_adjusted, df/(rho_adjusted*M))
        val betanew: DenseVector[Double] = DenseVector.vertcat(DenseVector(beta_0), beta_1_to_p)
        beta = betanew

        // Todo: implement r_b, beta_b, u_b, eta_b update from D:RDD[]
        // operation on each block of data
//        D = D.map(a => {
//        })
      }
    }
    Array(0.0)
  }

  /**
   *
   * @param n number of standard normal random numbers
   * @return a DenseVector (of length n) of standard normal random numbers
   */
  def randomVector(n: Int): DenseVector[Double] = {
    DenseVector.rand(n)
  }

}
