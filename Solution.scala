/*
import java.io._

import java.util._

import Solution._

//remove if not needed
import scala.collection.JavaConversions._

object Solution {

  def main(args: Array[String]) = {
    val in: Scanner = new Scanner(System.in)
    var listOfCases: Int = java.lang.Integer.parseInt(in.nextLine())
    val sol: Solution = new Solution()
    while (listOfCases > 0) {
      val result: Int = sol.checkCondition(in.nextLine())
      println(result) { listOfCases -= 1; listOfCases + 1 }
    }
  }

}

class Solution {

  def checkCondition(str: String): Int = {
    if (str.length % 2 == 1) {
      -1
    }
    val lstOne: String = str.substring(0, str.length / 2)
    val lsttwo: String = str.substring(str.length / 2)
    val arr: Array[Int] = Array.ofDim[Int](26)
    for (i <- 0 until lstOne.length) {
      val charOne: Char = lstOne.charAt(i)
      val chartwo: Char = lsttwo.charAt(i)
      if (charOne < 'a' || charOne > 'z' || chartwo < 'a' || chartwo > 'z') {
        -1
      }
      { arr(charOne - 'a') += 1; arr(charOne - 'a') - 1 }
      { arr(chartwo - 'a') -= 1; arr(chartwo - 'a') + 1 }
    }
    var count: Int = 0
    for (num <- arr) {
      count += Math.abs(num)
    }
    count / 2
  }

}
*/
