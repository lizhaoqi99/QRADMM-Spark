import breeze.linalg._
import breeze.plot._
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
    sc.setCheckpointDir("checkpoint")

    var arr = ofDim[Double](p+2, n)
    for(i <- 0 to p+1) {
      val temp = Xy(::, i).toArray
      arr(i) = temp
    }
    arr = arr.transpose

    val data = sc.parallelize(arr)

    val step = 0.1
    var beta: Array[Double] = Array(0)
    val beta_arr: Array[Array[Double]] = Array.ofDim[Double](10, p+1)
    spark.time(
      beta = QRADMM_Lasso.solve(data, 0.9, 100, 1, 5, M = 20)
    )

/*
    var i = 0.01
    var i_rho = 0.5
    var quantiles = Array(i)
    var j = 0
    while(i < 1) {
      beta = QRADMM_Lasso.solve(data, i, 100, i_rho, 1.5, M = 20)
//      beta.foreach(println)
//      println()
//      println("--------------")
      beta_arr(j) = beta
      j += 1
      i += step
      i_rho += 0.3
      quantiles :+= i
    }
//    beta_arr.foreach(println)
    print(beta_arr.map(_.mkString("  ")).mkString("\n"))
    println("--------------")

    val intercept = beta_arr.map(arr => arr(0))
    println("Intercept printing:")
    intercept.foreach(println)
    println("--------------")
//    println("Quantiles printing:")
    quantiles = quantiles.slice(0, quantiles.size - 1)
//    quantiles.foreach(println)

    val fig1 = Figure()
    val p1 = fig1.subplot(0)
    p1 += plot(quantiles, intercept)
    p1 += plot(quantiles, intercept, '.')
    p1.xlabel = "quantiles"
    p1.ylabel = "intercept"
    p1.setXAxisDecimalTickUnits()
    p1.setYAxisDecimalTickUnits()

    fig1.refresh()

    val fig2 = Figure()
    val x1 = beta_arr.map(arr => arr(1))
    val p2 = fig2.subplot(0)
    p2 += plot(quantiles, x1)
    p2 += plot(quantiles, x1, '.')
    p2.xlabel = "quantiles"
    p2.ylabel = "x1"
    p2.setXAxisDecimalTickUnits()
    p2.setYAxisDecimalTickUnits()
    fig2.refresh()

    val fig3 = Figure()
    val x2 = beta_arr.map(arr => arr(2))
    val p3 = fig3.subplot(0)
    p3 += plot(quantiles, x2)
    p3 += plot(quantiles, x2, '.')
    p3.xlabel = "quantiles"
    p3.ylabel = "x2"
    p3.setXAxisDecimalTickUnits()
    p3.setYAxisDecimalTickUnits()
    fig3.refresh()

    val fig4 = Figure()
    val x3 = beta_arr.map(arr => arr(3))
    val p4 = fig4.subplot(0)
    p4 += plot(quantiles, x3)
    p4 += plot(quantiles, x3, '.')
    p4.xlabel = "quantiles"
    p4.ylabel = "x3"
    p4.setXAxisDecimalTickUnits()
    p4.setYAxisDecimalTickUnits()
    fig4.refresh()

    val fig5 = Figure()
    val x4 = beta_arr.map(arr => arr(4))
    val p5 = fig5.subplot(0)
    p5 += plot(quantiles, x4)
    p5 += plot(quantiles, x4, '.')
    p5.xlabel = "quantiles"
    p5.ylabel = "x4"
    p5.setXAxisDecimalTickUnits()
    p5.setYAxisDecimalTickUnits()

    fig5.refresh()

*/
    spark.stop()

  }
}
