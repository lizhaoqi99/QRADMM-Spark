import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object run {

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

//    Array(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0), Array(4.0, 5.0, 7.0), Array(4.0, 5.0, 8.0))
//    val E: RDD[(Int, DenseMatrix[Double])] =
//      rdd.mapPartitionsWithIndex((i,a) => {
//        val ar = a.toArray
//        val mat = DenseMatrix(ar:_*)
//        Iterator((i,mat))
//      }).persist()
//
//    E.collect().foreach(a => {
//      println(a._1)
//      println(a._2)
//    })

    println()

    val F: RDD[(Int, DenseVector[Double])] =
      rdd.mapPartitionsWithIndex((i,a) => {
        val ar = a.toArray
        val vector = DenseVector.zeros[Double](6)
        var j = 0
        ar.foreach(elem => {
          elem.foreach(num => {
            vector(j) = num
            j = j + 1
          })
        })
        Iterator((i, vector))
      })

    F.collect().foreach(a => {
      println(a._1)
      println(a._2)
    })

    println("temp:")
    val temp = F.map(a=>a._2).reduce(_+_)
    println(temp)
    println("mean(temp):")
    println(temp/2.0)
    println(temp(1 to 3))
    println(temp.slice(1,4))

    val tempval = DenseVector(1.0)
    val temp2 = DenseVector.vertcat(tempval,temp)
    println(temp2)
    //    val vec2 = vec.asDenseMatrix.t
    //    val vec3 = DenseVector(9.0,10.0)
    //    val mat2 = DenseMatrix.horzcat(mat, vec2, vec3.asDenseMatrix.t)
    //    println(mat2)

    spark.stop()
  }
}
