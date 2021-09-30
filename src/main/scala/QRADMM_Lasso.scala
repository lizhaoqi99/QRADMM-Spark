import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    val lambdau = lambda/n
    val rhou = rho/n
    val data = x_y.repartition(M)

    // calculation will be done by partition/block
    // D: RDD[(partition index, (x_b, y_b, beta_b, r_b, u_b, eta_b))]
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
    val betaold = DenseVector[Double](p + 1)

    breakable{
      for (curr_iter <- 1 to max_iter) {

//      old_beta = beta
      //      D = D.map(a => {
      //      })


      //      for (j <- Seq(1,p)) {
      //        if(j == 0) {
      ////          beta(0) = sum(y-x*beta)/n
      //        }
      //      }

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

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QRADMM")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val arr = Array(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0), Array(4.0, 5.0, 7.0), Array(4.0, 5.0, 8.0))
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


    // D = ((1,2),3), ((4,5),6), ((4,5),7), ((4,5),8)
    // big RDD initialization step
//    val C: RDD[(Int, DenseVector[Double])] =
//      D.mapPartitionsWithIndex((i,a) => {
//        val ar = a.toArray
//        val matr = DenseMatrix.zeros[Double](4,4)
//        val vec1 = DenseVector.zeros[Double](4)
//        //        val vec2 = DenseVector.zeros[Double](2)
//        var j = 0
//        ar.foreach(arr => {
//          vec1(j) = arr._2
//          j = j + 1
//        })
//        Iterator((i, vec1))
//      }).persist()

//    println(C.count())
//    C.collect().foreach(a => {
//      println(a._1)
//      println(a._2)
//    })

    val E: RDD[(Int, DenseMatrix[Double])] =
      rdd.mapPartitionsWithIndex((i,a) => {
        val ar = a.toArray
        val mat = DenseMatrix(ar:_*)
        Iterator((i,mat))
      }).persist()

    E.collect().foreach(a => {
      println(a._1)
      println(a._2)
    })

    println()

    val vec2 = vec.asDenseMatrix.t
    val vec3 = DenseVector(9.0,10.0)
    val mat2 = DenseMatrix.horzcat(mat, vec2, vec3.asDenseMatrix.t)
    println(mat2)




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
