package hw1

import scala.annotation.tailrec

sealed trait Option[+A] {
  def map[B](f: A => B): Option[B] = if (isEmpty) None else Some(f(this.get))

  def flatMap[B](f: A => Option[B]): Option[B] = if (isEmpty) None else f(this.get)

  def getOrElse[B >: A](default: => B): B = if (isEmpty) default else this.get

  def orElse[B >: A](ob: => Option[B]): Option[B] = if (isEmpty) ob else this

  def filter(f: A => Boolean): Option[A] = if (isEmpty || f(this.get)) this else None

  def isEmpty: Boolean

  def get: A
}

object Option {
  def empty[A]: Option[A] = None
}

case class Some[+A](get: A) extends Option[A] {
  override def isEmpty: Boolean = false
}

case object None extends Option[Nothing] {
  override def isEmpty: Boolean = true

  override def get = throw new NoSuchElementException("None.get")
}

object OptionTest extends App {

  def variance(xs: Seq[Double]): Option[Double] = {
    if (xs.nonEmpty) {
      val m = xs.sum / xs.size
      Some(xs.map(el => math.pow(el - m, 2)).sum / xs.size)
    } else {
      None
    }
  }

  def lift[A, B](f: A => B): Option[A] => Option[B] = _ map f

  def map2[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] =
    a.flatMap(aa => b.map(bb => f(aa, bb)))

  def sequence[A](a: List[Option[A]]): Option[List[A]] = {

    @tailrec
    def f(xs: List[Option[A]], res: Option[List[A]]): Option[List[A]] = xs match {
      case Some(head) :: tail => f(tail, res.map(l => head :: l).orElse(Some(head :: Nil)))
      case None :: _ => None
      case Nil => res
    }

    f(a, None)
  }


  def traverse[A, B](a: List[A])(f: A => Option[B]): Option[List[B]] = {
    @tailrec
    def func(xs: List[A], res: Option[List[B]]): Option[List[B]] = xs match {
      case head :: tail if f(head) != None => func(tail, res.map(l => f(head).get :: l).orElse(Some(f(head).get :: Nil)))
      case head :: _ if f(head) == None => None
      case Nil => res
    }

    func(a, None)
  }

  def Try[A](res: => A): Option[A] = {
    try {
      Some(res)
    } catch {
      case _: Exception => None
    }
  }

  def convertStringToInt(str: String): Option[Int] = {
    try Some(str.toInt)
    catch {
      case e: NumberFormatException => None
    }
  }


  val someList = List(Some(1), Some(2), Some(3))
  val noneList = List(Some(1), None, Some(3))

  val someList1 = List("1", "2", "3")
  val noneList1 = List("1", "hello", "3")

  //  println(sequence(someList))
  //  println(sequence(noneList))
  //
  //  println(traverse(someList1)(el => Try(el.toInt)))
  //  println(traverse(noneList1)(el => Try(el.toInt)))

  val res = convertStringToInt("hello")
  val res1 = res
    .map { r =>
      r + 15
    }
    .getOrElse(42)

  res match {
    case Some(value) => s"everything is ok - $value"
    case None => "error"
  }

  println(res1)

}