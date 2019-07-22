package RandomProg

object ScalaCollections extends App {


  val numbers = Seq(1,2,3,4,5,6,7,8,9,10)

  numbers.filter(n => n % 2 == 0).foreach(println)
  
  numbers.filterNot(n => n % 2 == 0).foreach(println)
}
