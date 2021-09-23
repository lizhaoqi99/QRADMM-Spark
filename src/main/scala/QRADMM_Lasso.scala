import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

object QRADMM_Lasso {

//  def soft_threshold(x: DenseVector[Double], lambda: Double, rho: Double): DenseVector[Double] = {
//    val kappa = lambda / rho
//    val zeros = DenseVector.zeros[Double](x.length)
//    max(zeros, x - kappa) - max(zeros, -x - kappa)
//  }

// p.32
  def soft_threshold(x: DenseVector[Double], y: DenseVector[Double]): DenseVector[Double] = {
    val zeros = DenseVector.zeros[Double](x.length)
    max(zeros, x - y) - max(zeros, -x - y)
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
   * @param num_blocks    number of partitions of the original data
   * @return vector of coefficient estimates of the linear quantile regression model
   */
  def betaUpdate_lasso(x: RDD[Array[Double]], y: DenseVector[Double], beta: DenseVector[Double], max_iter: Int = 1000, rho: Double, lambda: Double, e_abs: Double = 1E-5, e_rel: Double = 1E-5, num_blocks: Int): Array[Double] = {

    val sc = x.sparkContext
    val xmat = DenseMatrix(x.collect():_*) // https://stackoverflow.com/questions/48166561/create-a-breeze-densematrix-from-a-list-of-double-arrays-in-scala
    val xtmat = xmat.t
    val xt = sc.parallelize(x.collect().toArray.transpose)
    var old_beta = beta
    val p = x.first().length - 1  // assume x includes leading 1's
    val n = x.count()
    var curr_iter = 1
    val data = xt.repartition(num_blocks) // use transpose because we want to iterate over j=0...p
    val r = y-xmat*beta

    /* TODO: wrap everything (x,y,beta,...) into a RDD and utilize map() */

    // put everything in RDD so that we can correspond each element by index (xj, betaj, etc...)
    // val collection = (xtmat, sj, betaj, rj=(betaold(j)-beta(j))*xj)
    // D represents the whole for loop where j <- 0 to p

    // calculation will be done by partition/block
    // so we need to consider if the updates (betaj, sj, ...) can be done in a block fashion
    // and if we make num_block=p, everything can be done in parallel
    // -> repartition by column (across p columns)
    val D: RDD[(Int, (DenseMatrix[Double], DenseVector[Double], DenseVector[Double], DenseVector[Double]))] =
    data.mapPartitionsWithIndex((i,xj) => {
//      val xj_vec = xj.toArray
//      val beta0 =
//      val sj = r + (old_beta(0) - beta(0)) * x
      // TODO: this current implementation is INCORRECT since num_block NOT fixed to p
      val sj = DenseVector[Double](n)
      val betaj = DenseVector[Double](n)
      val rj = DenseVector[Double](n)
      Iterator((i, (xtmat, sj, betaj, rj)))
    }).persist()

    // TODO: update checkpoint directory
    D.checkpoint()  // break RDD lineage at each iteration

    while(curr_iter <= max_iter) {
      old_beta = beta
//      D = D.map(a => {
//      })


//      for (j <- Seq(1,p)) {
//        if(j == 0) {
////          beta(0) = sum(y-x*beta)/n
//        }
//      }
    }
    Array(0.0)
  }

  // we can take advantage of big RDD's in the main function
  // and we can also call broadcast() on r updates since we're applying the soft threshold function to it
  def solve(): Unit = {
    // RDD[(partition number, (sj, beta, r))]
    //      val C: RDD[(Int, (DenseVector[Double], DenseVector[Double], DenseVector[Double]))] =
    //      {
    //        data.mapPartitionsWithIndex((i,xj) => {
    //          val xj_vec = DenseVector(xj.toArray)
    //          val beta0 =
    //          val sj = r + (old_beta(0) - beta(0)) * x
    //          Iterator((i, (xj_vec)))
    //        })
    //      }
    //      C.checkpoint()  // break RDD lineage at each iteration
  }

  /**
   *
   * @param n number of standard normal random numbers
   * @return a DenseVector (of length n) of standard normal random numbers
   */
  def randomVector(n: Int): DenseVector[Double] = {
    DenseVector.rand(n)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QRADMM")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val arr = Array(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0))
    val data: RDD[Array[Double]] = sc.parallelize(arr)
    val rdd = data.repartition(2)
    rdd.collect().foreach(x => println(x.mkString(", ")))

    val p = rdd.first().length
//    println(p)
    val n = rdd.count()
//    println(n)

    val D = rdd.map(row=>(row.slice(0,p-1), row(p-1)))
    D.collect().foreach(a => println(a._1.mkString(", ")))

    val mat: DenseMatrix[Double] = DenseMatrix((1.0,2.0),(4.0,5.0))
    val vec: DenseVector[Double] = DenseVector(7.0,8.0)
    val res = mat * vec
//    println(res)
    println(vec(1))

    val d = data.collect()
//    d.foreach(a => println(a.mkString(", ")))

//    val ad = DenseMatrix(data.collect():_*)
//    println(ad)



    // big RDD initialization step
    val C: RDD[(Int, DenseVector[Double])] =
      D.mapPartitionsWithIndex((i,a) => {
        val ar = a.toArray
        val vec1 = DenseVector.zeros[Double](2)
        //        val vec2 = DenseVector.zeros[Double](2)
        var j = 0
        ar.foreach(arr => {
          vec1(j) = arr._2
          j = j + 1
        })
        Iterator((i, vec1))
      }).persist()





    println(C.count())
    C.collect().foreach(a => {
      println(a._1)
      println(a._2)
    })


//      .mapPartitionsWithIndex((i,a) => {
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

      // A, b, x, u, r
//      Iterator((i, (pinv(A.t*A+DenseMatrix.eye[Double](p-1)), A.t*b, DenseVector.rand(p-1), DenseVector.rand(p-1), DenseVector.rand(p-1))))
//    }).persist()



    spark.stop()
  }
}
