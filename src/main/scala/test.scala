object test {
  def main(args: Array[String]): Unit = {
    val arr = Array(Array(1.0,2.0), Array(3.0,4.0))

    val temp = arr.map(ar => ar(0))
    temp.foreach(println)

    var temp1 = Array(1, 2)
//    val temp1_buffer = temp1.toBuffer
//    temp1_buffer.remove(-1)
//    temp1 = temp1_buffer.toArray
    temp1 = temp1.slice(0, temp1.size - 1)
    temp1.foreach(println)
  }
}
