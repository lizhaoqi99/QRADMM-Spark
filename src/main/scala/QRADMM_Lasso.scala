import breeze.linalg._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

object QRADMM_Lasso {

  /**
   *
   * Assume the intercept is included in the model so that the first column of the design matrix x is a vector of 1
   * @param x_y      a rdd collection consists of the design matrix x and the response y
   * @param tau      quantile of interest
   * @param max_iter maximum number of iterations
   * @param rho      augmentation parameter
   * @param lambda   penalty parameter
   * @param ABSTOL   absolute tolerance stopping constant
   * @param RELTOL   relative tolerance stopping constant
   * @param M        number of partitions of the original data
   * @return vector of coefficient estimates of the linear quantile regression model
   */
  def solve(x_y: RDD[Array[Double]], tau: Double, max_iter: Int = 100, rho: Double, lambda: Double, ABSTOL: Double = 1E-7, RELTOL: Double = 1E-4, M: Int): Array[Double] = {

    val sc = x_y.sparkContext
    val p = x_y.first().length - 2  // assume x includes leading 1's
    val n = x_y.count().toInt

//    val original_xy = DenseMatrix(x_y.collect():_*)
//    val original_x = original_xy(::, 0 to p)
//    val original_y = original_xy(::, p+1)

//    val ni = n/M
    val lambda_adjusted = lambda/n
    val rho_adjusted = rho/n
    val data = x_y.repartition(M)
    // x and y after shuffling
    val xy = DenseMatrix(data.collect():_*)
    val x = xy(::, 0 to p)
    val y = xy(::, p+1)

//    print("Printing shuffled data:\n")
//    print(xy)
//    print("\n")
    // calculation will be done by partition/block
    // D: RDD[(partition index, (x_b, y_b, beta_b, r_b, u_b, eta_b)]
    // length of D is M

    var D: RDD[(Int, (DenseMatrix[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double]))] = {
      data.mapPartitionsWithIndex((i, x_y_block) => {
        val x_y_b = DenseMatrix(x_y_block.toArray: _*) // https://stackoverflow.com/questions/48166561/create-a-breeze-densematrix-from-a-list-of-double-arrays-in-scala
//        print("loop!!!\n")
//        println(x_y_b)
        val x_b = x_y_b(::, 0 to p)
        val y_b = x_y_b(::, p + 1)
        // r_b: DenseVector[Double](n / M)
//        val r_b = y.slice(n/M*i,n/M*i+n/M)
        val r_b = y_b
        Iterator((i, (x_b, y_b, DenseVector.zeros[Double](p + 1), r_b, DenseVector.zeros[Double](n / M), DenseVector.zeros[Double](p + 1))))
      }).cache()
    }

//    print("Printing Old D:\n")
//    D.collect().foreach(a => {
//      println(a._1)
//      println(a._2)
//    })

    D.checkpoint()  // break RDD lineage at each iteration

//    val r = y-xmat*beta
    var beta = DenseVector.zeros[Double](p + 1)
    var betaold = beta
    var rnorm = 0.0
    var snorm = 0.0
    var e_pri = 0.0
    var e_dual = 0.0
    var niter = 0

    breakable {
      for (curr_iter <- 1 to max_iter) {
        niter = curr_iter
//        println(curr_iter)
//        val t1 = System.nanoTime()
        val beta_avg = D.map(a => a._2._3).reduce(_ + _) / M.toDouble
//        println("DMap")
//        D.map(a=>a._2._3).foreach(println)

        val eta_avg = D.map(a => a._2._6).reduce(_ + _) / M.toDouble
//        val t2 = System.nanoTime()
//        val diff = (t2-t1)/1e9d
//        println(s"The program took $diff seconds.")
        betaold = beta
//        println(beta_avg)
//        println(eta_avg)

        // update beta
        val df = DenseVector.fill(p, lambda_adjusted)

        val beta_0 = beta_avg(0) + eta_avg(0)/rho_adjusted
//        println(s"beta: ${beta_avg.slice(1,p+1)}")
//        println(df)
//        println(s"whole: ${beta_avg.slice(1,p+1)+eta_avg.slice(1,p+1)/rho_adjusted}")
//        println("df/(rho_adj*M)")
//        println(df/(rho_adjusted*M))
        val beta_1_to_p = soft_threshold(beta_avg.slice(1,p+1)+eta_avg.slice(1,p+1)/rho_adjusted, df/(rho_adjusted*M))

//        println("Beta[1:p]")
//        println(beta_1_to_p)
        val betanew: DenseVector[Double] = DenseVector.vertcat(DenseVector(beta_0), beta_1_to_p)
        beta = betanew
//        println(beta)

        D.unpersist()
        // Todo: implement r_b, beta_b, u_b, eta_b update from D:RDD[]; use broadcast() in the r-update
        // operation on each block of data
        D = D.map(a => {
          // parse all the parameters
          val x_b = a._2._1
          val y_b = a._2._2
          val beta_b = a._2._3
          val r_b = a._2._4
          val u_b = a._2._5
          val eta_b = a._2._6
          val xbeta_b = x_b * beta_b

          // update r_b
          val temp = DenseVector.fill(n/M, 0.5/(n*rho_adjusted))
          val r_b_new = soft_threshold(u_b/rho_adjusted+y_b-xbeta_b-0.5*(2*tau-1)/(n*rho), temp)
//          // update beta_b
//          println("hello")
//          println(x_b)
//          println("hellofasf")
//          println(x_b.t*(y_b-r_b_new+u_b/rho_adjusted)-eta_b/rho_adjusted+beta)
          val beta_b_new = inv(x_b.t*x_b+DenseMatrix.eye[Double](p+1)) * (x_b.t*(y_b-r_b_new+u_b/rho_adjusted)-eta_b/rho_adjusted+beta)
//          val beta_b_new = x_b.t*(y_b-r_b_new+u_b/rho_adjusted)-eta_b/rho_adjusted+beta_b
          // update u_b
          val u_b_new = u_b + rho_adjusted*(y_b-xbeta_b-r_b_new)
          // update eta_b
          val eta_b_new = eta_b + rho_adjusted*(beta_b_new-beta)
          (a._1, (x_b,y_b,beta_b_new,r_b_new,u_b_new,eta_b_new))
        })

        D.cache()
        // override last RDD at each iteration
        D.checkpoint()

//        println(s"curr_iter: $curr_iter")
//        print("Printing New D:\n")
//        D.collect().foreach(a => {
//          println(a._1)
//          println(a._2._2)
//          println(a._2._3)
//        })
//      }
//    }
//        val t1 = System.nanoTime()
        // collect r_b from each block and form r which has length n
        val r = DenseVector(D.map(a => a._2._4).collect().flatMap(x => x.toArray))
        // collect u_b from each block and form u which has length n
        val u = DenseVector(D.map(a => a._2._5).collect().flatMap(x => x.toArray))
//        val t2 = System.nanoTime()
//        val diff = (t2-t1)/1e9d
//        println(s"The program took $diff seconds.")

        rnorm = math.sqrt(sum(square(y-x*beta)))
        snorm = math.sqrt(sum(square(rho_adjusted*x(::, 1 to p)*(beta.slice(1,p+1)-betaold.slice(1,p+1)))))
        val cand: DenseVector[Double] = DenseVector.zeros(3)
        cand(0) = math.sqrt(sum(square(x(::, 1 to p)*beta.slice(1,p+1))))
        cand(1) = math.sqrt(sum(square(-r)))
        cand(2) = math.sqrt(sum(square(y-beta(0))))

        // feasibility tolerance for primal feasibility condition
        e_pri = math.sqrt(n)*ABSTOL + RELTOL*max(cand)
        // feasibility tolerance for dual feasibility condition
        e_dual = math.sqrt(n)*ABSTOL + RELTOL*math.sqrt(sum(square(u)))

        // check stopping criterion
        if(rnorm <= e_pri && snorm <= e_dual) {
          break
        }
      }
    }
//    println(s"It takes $niter iterations to converge")

    beta.toArray

//    Array(1.0)
  }

  // p.32
  def soft_threshold(x: DenseVector[Double], y: DenseVector[Double]): DenseVector[Double] = {
    val zeros = DenseVector.zeros[Double](x.length)

    max(zeros, x - y) - max(zeros, -x - y)
  }

  /**
   *
   * @param v DenseVector to be squared element-wise
   * @return squared DenseVector
   */
  def square(v: DenseVector[Double]): DenseVector[Double] = {
    v*v
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
