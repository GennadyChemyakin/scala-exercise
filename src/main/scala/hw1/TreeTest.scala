package hw1

object TreeTest extends App {

  sealed trait Tree[+A]

  case class Leaf[A](value: A) extends Tree[A]

  case class Branch[A](left: Tree[A], right: Tree[A]) extends Tree[A]

  def size[A](tree: Tree[A]): Int = tree match {
    case Leaf(_) => 1
    case Branch(left, right) => size(left) + size(right) + 1
  }

  def max(tree: Tree[Int]): Int = {
    def func(tree: Tree[Int], m: Int): Int = tree match {
      case Leaf(value) => value max m
      case Branch(left, right) => func(left, m) max func(right, m)
    }

    func(tree, Int.MinValue)
  }

  def depth[A](tree: Tree[A]): Int = {
    def func(tree: Tree[A], d: Int): Int = tree match {
      case Leaf(_) => d + 1
      case Branch(left, right) => func(left, d + 1) max func(right, d + 1)
    }

    func(tree, 0)
  }

  def map[A, B](tree: Tree[A])(f: A => B): Tree[B] = tree match {
    case Leaf(value) => Leaf(f(value))
    case Branch(left, right) => Branch(map(left)(f), map(right)(f))
  }

  def fold[A, B](tree: Tree[A], z: B)(f: (A, B) => B)(f1: (B, B) => B): B = tree match {
    case Leaf(value) => f(value, z)
    case Branch(left, right) => f1(fold(left, z)(f)(f1), fold(right, z)(f)(f1))
  }

  def mapF[A, B](tree: Tree[A])(f: A => B): Tree[B] = {
    fold[A, Tree[B]](tree, null)((a, _) => Leaf(f(a)))((l, r) => Branch(l, r))
  }

  def sizeF[A](tree: Tree[A]): Int = {
    fold(tree, 0)((_, _) => 1)((l, r) => l + r + 1)
  }

  def maxF(tree: Tree[Int]): Int = {
    val findMax = (v: Int, m: Int) => v max m
    fold(tree, Int.MinValue)(findMax)(findMax)
  }

  def depthF[A](tree: Tree[A]): Int = {
    fold(tree, 0)((_, _) => 1)((l, r) => l max r + 1)
  }

  val tree = Branch(
    Branch(
      Branch(
        Leaf(1),
        Leaf(2)
      ),
      Branch(
        Leaf(3),
        Leaf(4)
      )
    ),
    Branch(
      Branch(
        Leaf(5),
        Leaf(6)
      ),
      Branch(
        Leaf(7),
        Branch(
          Leaf(8),
          Leaf(9)
        )
      )
    ),
  )

  val size: Int = size(tree)
  val sizeF: Int = sizeF(tree)

  val max: Int = max(tree)
  val maxF: Int = maxF(tree)

  val depth: Int = depth(tree)
  val depthF: Int = depthF(tree)

  val sum: Int = fold(tree, 0)(_ + _)(_ + _)

  val m: Tree[Int] = map(tree)(_ * 2)
  val mF = mapF(tree)(_ * 2)

  println(size)
  println(sizeF)
  println("/////////")
  println(max)
  println(maxF)
  println("/////////")
  println(depth)
  println(depthF)
  println("/////////")
  println(sum)
  println("/////////")
  println(m)
  println(mF)

}
