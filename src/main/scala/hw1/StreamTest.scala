package hw1

import hw1.Stream.cons
import hw1.StreamTest.{unfold, zipAll}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Try

sealed trait Stream[+A] {
  def headOption: Option[A] = this match {
    case Empty => None
    case Cons(head, _) => Some(head())
  }

  def toList: List[A] = {

    val a = (xs: ListBuffer[A], v: A) => {
      println(s"toList $v")
      xs += v
    }

    @tailrec
    def f(s: Stream[A], res: ListBuffer[A]): ListBuffer[A] = s match {
      case Empty => res
      case Cons(head, tail) => f(tail(), a(res, head()))
    }

    f(this, ListBuffer[A]()).toList
  }

  def take(n: Int): Stream[A] = {
    @tailrec
    def f(s: Stream[A], res: Stream[A], count: Int = 0): Stream[A] = s match {
      case Empty | Cons(_, _) if count >= n => res
      case Cons(head, tail) if count < n => f(tail(), Cons(head, () => res), count + 1)
    }

    f(this, Empty)
  }

  def take_(n: Int): Stream[A] =
    if (n <= 0) {
      Empty
    } else {
      this match {
        case Empty => Empty
        case Cons(head, tail) => cons(head(), tail() take_ n - 1)
      }
    }

  def drop(n: Int): Stream[A] = {
    @tailrec
    def f(s: Stream[A], count: Int = 0): Stream[A] = s match {
      case Empty | Cons(_, _) if count >= n => s
      case Cons(_, tail) if count < n => f(tail(), count + 1)
    }

    f(this)
  }

  def takeWhile(p: A => Boolean): Stream[A] = {
    this match {
      case Cons(head, tail) if p(head()) => cons(head(), tail().takeWhile(p))
      case _ => Empty
    }
  }

  def foldRight[B](z: => B)(f: (A, => B) => B): B = this match {
    case Cons(head, tail) => f(head(), tail().foldRight(z)(f))
    case _ => z
  }

  def exists(p: A => Boolean): Boolean = foldRight(false)((a, b) => p(a) || b)

  def forAll(p: A => Boolean): Boolean = foldRight(true)((a, b) => p(a) && b)

  def takeWhileF(p: A => Boolean): Stream[A] = {
    foldRight(Stream.empty[A]) { (a, b) =>
      if (p(a)) Cons(() => a, () => b)
      else Empty
    }
  }

  def headOptionF: Option[A] = foldRight(Option.empty[A])((a, _) => Some(a))

  def map[B](f: A => B): Stream[B] = foldRight(Stream.empty[B]) { (a, b) =>
    println(s"map $a")
    Cons(() => f(a), () => b)
  }

  def filter(p: A => Boolean): Stream[A] = foldRight(Stream.empty[A]) { (a, b) =>
    println(s"filter $a")
    if (p(a)) Cons(() => a, () => b)
    else b
  }

  def append[B >: A](value: Stream[B]): Stream[B] = foldRight(value)((a, b) => Cons(() => a, () => b))


  def flatMap[B](f: A => Stream[B]): Stream[B] = foldRight(Stream.empty[B])((a, b) => f(a).append(b))


  def find(p: A => Boolean): Option[A] = this.filter(p).headOptionF


  def startsWith[B](s1: Stream[B]): Boolean =
    unfold(zipAll(this, s1)) {
      case Cons(head, tail) =>
        head() match {
          case (Some(value0), Some(value1)) if value0 == value1 => Some(true, tail())
          case (Some(_), None) => Some(true, Stream.empty)
          case _ => Some(false, Stream.empty)
        }
      case _ => None
    }.find(_ == false).getOrElse(true)
}

case object Empty extends Stream[Nothing]

case class Cons[+A](h: () => A, t: () => Stream[A]) extends Stream[A]

object Stream {
  def cons[A](hd: => A, t1: => Stream[A]): Stream[A] = {
    lazy val head = hd
    lazy val tail = t1
    Cons(() => head, () => tail)
  }

  def empty[A]: Stream[A] = Empty

  def apply[A](as: A*): Stream[A] = if (as.isEmpty) empty else cons(as.head, apply(as.tail: _*))
}


object StreamTest extends App {

  def constant[A](a: A): Stream[A] = {
    lazy val inf: Stream[A] = Stream.cons(a, inf)
    inf
  }

  def from(n: Int): Stream[Int] = {
    lazy val inf: Stream[Int] = Stream.cons(n, inf.map(_ + 1))
    inf
  }

  def fibs: Stream[Int] = {
    def go(f0: Int, f1: Int): Stream[Int] = Stream.cons(f0, go(f1, f0 + f1))

    go(0, 1)
  }

  def unfold[A, S](z: S)(f: S => Option[(A, S)]): Stream[A] =
    f(z) match {
      case Some((value, state)) => Stream.cons(value, unfold(state)(f))
      case None => Empty
    }

  def fibsUF: Stream[Int] = Stream(0).append(Stream(1)).append(unfold((0, 1)) {
    case (f0, f1) => Some(f0 + f1, (f1, f0 + f1))
  })

  def fromUF(n: Int): Stream[Int] = unfold(n)(a => Some((a, a + 1)))

  def constantUF(n: Int): Stream[Int] = unfold(n)(a => Some((a, a)))

  def onesUF: Stream[Int] = constantUF(1)

  def map[A, B](s: Stream[A])(f: A => B): Stream[B] = unfold(s) {
    case Cons(head, tail) => Some(f(head()), tail())
    case Empty => None
  }

  def takeUF[A](s: Stream[A], n: Int): Stream[A] = unfold((s, n)) {
    case (Cons(head, tail), a) if a > 0 => Some(head(), (tail(), a - 1))
    case _ => None
  }

  def takeWhileUF[A](s: Stream[A])(p: A => Boolean): Stream[A] = unfold(s) {
    case Cons(head, tail) if p(head()) => Some(head(), tail())
    case _ => None
  }

  def zipWith[A, B](s0: Stream[A], s1: Stream[A])(func: (A, A) => B): Stream[B] = unfold((s0, s1)) {
    case (Cons(head0, tail0), Cons(head1, tail1)) => Some(func(head0(), head1()), (tail0(), tail1()))
    case _ => None
  }

  def zipAll[A, B](s0: Stream[A], s1: Stream[B]): Stream[(Option[A], Option[B])] = unfold((s0, s1)) {
    case (Cons(head0, tail0), Cons(head1, tail1)) => Some((Some(head0()), Some(head1())), (tail0(), tail1()))
    case (Empty, Cons(head1, tail1)) => Some((None, Some(head1())), (Empty, tail1()))
    case (Cons(head0, tail0), Empty) => Some((Some(head0()), None), (tail0(), Empty))
    case _ => None
  }

  def startsWith[A, B](s0: Stream[A], s1: Stream[B]): Boolean =
    unfold(zipAll(s0, s1)) {
      case Cons(head, tail) =>
        head() match {
          case (Some(value0), Some(value1)) if value0 == value1 => Some(true, tail())
          case (Some(_), None) => Some(true, Stream.empty)
          case _ => Some(false, Stream.empty)
        }
      case _ => None
    }.find(_ == false).getOrElse(true)


  val s = Stream(1, 2, 3, 4, 5)
  val inf: Stream[Int] = constant(1)

  //  println(s.toList)
  //  println(s.take(3).toList)
  //  println(s.take_(3).toList)
  //  println(s.drop(3).toList)
  //  println(s.takeWhile(_ < 4).toList)
  //  println(s.takeWhileF(_ < 4).toList)
  //  println(s.headOptionF)
  //  println(s.map(_ * 2).toList)
  //  println(s.filter(_ % 2 == 0).toList)
  //  println(s.append(Stream(6)).toList)
  //  println(s.flatMap(i => Stream(i, i)).toList)

  //  private val res = s.map(_ + 10).filter(_ % 2 == 0).toList
  //  println(res)


  //  println(inf.take(5).toList)
  //  println(from(5).take_(10).toList)
  //  println(fibs.take_(10).toList)

  //  println(unfold((0, 1, 10)) {
  //    case (f0, f1, n) if n > 0 => Some(f0 + f1, (f1, f0 + f1, n - 1))
  //    case _ => None
  //  }.take_(10).toList)

  println(fibsUF.take_(10).toList)
  println(fromUF(5).take_(10).toList)
  println(constantUF(5).take_(10).toList)
  println(map(s)(_ + 10).toList)
  println(takeUF(fibsUF, 10).toList)
  println(takeWhileUF(fibsUF)(_ < 14).toList)
  println(zipWith(Stream(1, 2, 3, 4, 5), Stream(1, 2, 3, 4, 5, 6))(_ + _).toList)
  println(zipAll(Stream(1, 2, 3, 4, 5), Stream(1, 2, 3, 4, 5, 6)).toList)
  println(startsWith(Stream(1, 2, 3, 4, 5, 6, 7), Stream(1, 2, 3, 4, 5, 6)))
  println(Stream(1, 2, 3, 4, 5, 6, 7, 8) startsWith Stream(11, 2, 3, 4, 5, 6))
}