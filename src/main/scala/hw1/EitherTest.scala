package hw1

import scala.annotation.tailrec

sealed trait Either[+E, +A] {

  def map[B](f: A => B): Either[E, B] = this match {
    case Right(value) => Right(f(value))
    case _ => this.asInstanceOf[Either[E, B]]
  }

  def flatMap[EE >: E, B](f: A => Either[EE, B]): Either[EE, B] = this match {
    case Right(value) => f(value)
    case _ => this.asInstanceOf[Either[E, B]]
  }

  def orElse[EE >: E, B >: A](b: => Either[EE, B]): Either[EE, B] = this match {
    case Left(_) => b
    case _ => this
  }

  def map2[EE >: E, B, C](b: Either[EE, B])(f: (A, B) => C): Either[EE, C] = (this, b) match {
    case (Left(l), _) => Left(l)
    case (_, Left(l)) => Left(l)
    case (Right(v0), Right(v1)) => Right(f(v0, v1))
  }
}

case class Left[+E](value: E) extends Either[E, Nothing]

case class Right[+A](value: A) extends Either[Nothing, A]

object EitherTest extends App {

  def Try[AA](a: => AA): Either[Exception, AA] = {
    try Right(a)
    catch {
      case e: Exception => Left(e)
    }
  }


  def parseInsuranceRateQuote(age: String,
                              numberOfSpeedingTickets: String): Either[Exception, Double] =
    for {
      a <- Try(age.toInt)
      tickets <- Try(numberOfSpeedingTickets.toInt)
    } yield (a + tickets) * 100


  def sequence[E, A](es: List[Either[E, A]]): Either[E, List[A]] = {
    @tailrec
    def f(xs: List[Either[E, A]], res: Either[E, List[A]]): Either[E, List[A]] = xs match {
      case Right(v) :: tail => f(tail, res.map(list => v :: list).orElse(Right(v :: Nil)))
      case Left(e) :: _ => Left(e).asInstanceOf[Either[E, List[A]]]
      case Nil => res
    }

    f(es, Right(Nil))
  }



  println(parseInsuranceRateQuote("15", "25"))
  println(parseInsuranceRateQuote("hello", "25"))

  println(sequence(List(Right(1), Right(2), Right(3))))
  println(sequence(List(Right(1), Left(new NumberFormatException), Right(3))))

  val res = Right(1)

  Try("hello".toInt) map(_ * 2) map (_ / 15)
}
