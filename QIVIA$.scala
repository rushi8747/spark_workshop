object QIVIA extends App {



  def gcd(a: Int, b: Int): Int = {
    if (b == 0) a else gcd(b, a % b)
  }

  val out = gcd(14, 21)
  println(out)

  def fact(n: Int):Int = {
     def loop(acc:Int,n:Int):Int=
      if(n == 0) acc else loop(acc*n , n-1)
    loop(1,n)
  }

  println(fact(4))


  def pascal(c: Int, r: Int): Int = {
    if (c == 0 || c == r) 1 else pascal(c - 1, r - 1) + pascal(c, r - 1)
  }

  println(pascal(0,3))

  def countChange(money: Int, coins: List[Int]): Int = {
    if (money == 0) 1
    else if (money < 0) 0
    else if (coins.isEmpty) 0
    else countChange(money - coins.head, coins) + countChange(money, coins.tail)
  }

  println(countChange(5,List(1,3)))

}
