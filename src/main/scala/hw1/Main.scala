package hw1

import java.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object Main extends App {

  val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[4]")
  val spark = new SparkContext(conf)


  def pif(): Unit = {

    val n = math.min(400000L, Int.MaxValue).toInt

    val pi = spark
      .parallelize(1 until n, 2)
      .map { _ =>
        val x = math.random * 2 - 1
        val y = math.random * 2 - 1

        if (x * x + y * y < 1) 1 else 0
      }
      .reduce(_ + _)

    val res: Double = 4 * pi / n


    println(res)
  }

  def wordCount(): Unit = {
    val words = spark
      .textFile("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/input", 1)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .cache()

    println(words.collect())

    val res = words.reduce((a, b) => if (a._2 > b._2) a else b)

    println(res)
  }

  def df(): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    import ss.implicits._

    val schema = Encoders.product[Test].schema

    val ds = ss
      .read
      .schema(schema)
      .csv("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/input")
      .as[Test]
      .repartition($"id")
      .cache()
  }

  def csvTest(): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("CSV test")
      .getOrCreate()

    import ss.implicits._

    val ds = ss
      .read
      .schema(Encoders.product[CsvTest].schema)
      .csv("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/test_csv")
      .as[CsvTest]
      .filter($"count" gt 60)

    ds.explain(extended = true)
  }

  def stocks(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    spark
      .read
      .option("header", value = true)
      .schema(Encoders.product[YahooStockPrice].schema)
      .csv("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/input_data/yahoo_stocks.csv")
      .write
      .orc("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/output_orc")

    spark
      .read
      .orc("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/output_orc")
      .createOrReplaceTempView("yahoo_orc_stocks")

    spark.sql("select * from yahoo_orc_stocks limit 50").show(false)
  }

  def stocksTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .orc("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/output_orc")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    df
      .withColumn("rank", rank().over(Window.orderBy($"high" desc)))
      .where($"rank" equalTo 1)
      .select($"date", $"high")
      .explain(true)

    //    maxDf.show(false)


    //    val df1 = spark.createDataFrame(
    //      spark.sparkContext.parallelize(Seq(Row(0L, "Ivan"))),
    //      StructType(Seq(StructField("id", LongType, nullable = false), StructField("name", StringType, nullable = false)))
    //    )
    //
    //    val df2 = spark.createDataFrame(Seq(Test(0L, "Ivan")))


  }

  def deduplicationTest(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions.{rank, concat, lit, split, expr, count, max}

    val ds = spark
      .read
      .schema(Encoders.product[CsvTest].schema)
      .csv("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/test_csv")
      .as[CsvTest]

    val res1 = ds
      .groupByKey(_.id)
      .mapGroups { (_, it) =>
        it.maxBy(_.count)
      }

    //    res1.explain(true)
    //    res1.show()


    val res2 = ds
      .withColumn("rank", rank().over(Window.partitionBy("id").orderBy($"count" desc)))
      .where($"rank" equalTo 1)
      .drop($"rank")
      .withColumn("is_ivan", $"name" isin("Ivan", "ivan"))
      .withColumn("mass[0]", expr("split(mass, ' ')[0]"))
      .select($"*", concat($"is_ivan" cast StringType, lit("_"), $"name") as "new_col")


    res2.explain(true)
    res2.printSchema()
    res2.show()
  }


  def test(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions.sum

    val df1 = spark.range(2, 1000000, 2)
    val df2 = spark.range(2, 1000000, 4)
    val step1 = df1.repartition(5)
    val step12 = df2.repartition(6)
    val step2 = step1.select($"id" * 5 as "id")
    val step3 = step2.join(step12, "id")
    val step4 = step3.select(sum($"id"))

    val res = step4.collect()

    step4.explain(true)
  }

  def futureTest(): Unit = {

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val f = Future {
      Thread.sleep(10000)
      println("done")
    }

    println(f)

    Await.result(f, 15 seconds)
    println(f)

    val f2 = Future {
      Thread.sleep(5000)

      throw new Exception("exception in future")
    }

    println(f2)

    Await.ready(f2, 15 seconds)

    println(f2)

    val scala.Some(res) = f2.value
    res match {
      case Success(r) => println(r)
      case Failure(exception) => println(exception.getMessage)
    }

    val fun: PartialFunction[Int, String] = {
      case 1 => "1"
    }

  }

  private def test4(): Unit = {
    val fun = (x: Double) => x * 3
    val pattern = "^\\s+([0-9]+)\\s+$".r
    val isMatched = (_: String).matches(pattern.toString())

    val fun2 = (x: Int) => (y: Int) => x * y
    //test()

    val pattern(num) = " 100 "

    val r1 = " 1000 " match {
      case pattern(n) => (n, "Success")
      case _ => ("0", "Failure")
    }

    val r3 = "-3+4".collect {
      case '+' => 1
      case '-' => -1
      case '3' => 3
    }

    val r = isMatched(" 100 ")

    val seq1 = 0 to 5 by 2
    val seq2 = Seq("Ivan", "Peter", "Scott", "John", "Barbara", "David")

    val r2 = seq1.collect(seq2)

    //  println(r)
    //  println(num)
    //  println(r1)
    //  println(r3)
  }

  def fibLoop(n: Int): Int = {
    val f0 = 0
    val f1 = 1
    var fn_2, fn_1, fn = 0

    n match {
      case 0 => f0
      case 1 => f1
      case x =>
        fn_2 = f0
        fn_1 = f1
        for (_ <- 2 to x) {
          fn = fn_2 + fn_1
          fn_2 = fn_1
          fn_1 = fn
        }
        fn
    }
  }

  def fib(n: Int): Int = {
    def f(n: Int, acc0: Int, acc1: Int): Int = n match {
      case 0 => acc0
      case x => f(x - 1, acc1, acc0 + acc1)
    }

    f(n, 0, 1)
  }

  @tailrec
  def isSorted[A: ClassTag](as: Array[A], ordered: (A, A) => Boolean): Boolean = {
    as match {
      case Array(_) => true
      case Array(el0, el1) => ordered(el0, el1)
      case Array(el0, el1, t@_*) => ordered(el0, el1) && isSorted(t.toArray, ordered)
    }
  }

  def partial1[A, B, C](a: A, f: (A, B) => C): B => C = {
    f.curried(a)
  }


  //  println(Range(0, 7).map(fibLoop))
  //  println(Range(0, 7).map(fib))

  val ord = (x: Int, y: Int) => x < y

  println(isSorted(Array(1, 2, 5, 3, 4), ord))

  Array(1, 2, 5, 3, 4).filter(_ / 2 == 0) map (_ * 15)


  def curry[A, B, C](f: (A, B) => C): A => B => C = a => b => f(a, b)

  def uncurry[A, B, C](f: A => B => C): (A, B) => C = (a, b) => f(a)(b)

  def compose[A, B, C](f: B => C, g: A => B): A => C = a => f(g(a))

  val f = (x: Double) => x + 1

  val f1 = f andThen (_ * 0)

  println(f1(5))

  @tailrec
  def drop[A](l: List[A], n: Int): List[A] = n match {
    case x if x <= 0 => l
    case x if x > l.size => Nil
    case _ => l match {
      case _ :: tail if n > 0 => drop(tail, n - 1)
    }
  }


  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    as match {
      case Nil => z
      case x :: xs => f(x, foldRight(xs, z)(f))
    }
  }


  def length[A](as: List[A]): Int = {
    val c = (_: Any, acc: Int) => acc + 1
    foldRight(as, 0)(c)
  }


  def foldLeft[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    @tailrec
    def func(as: List[A], acc: B): B = as match {
      case Nil => acc
      case head :: tail => func(tail, f(head, acc))
    }

    func(as, z)
  }

  def lengthFl[A](as: List[A]): Int = {
    val c = (_: Any, acc: Int) => acc + 1
    foldLeft(as, 0)(c)
  }

  def reverse[A](as: List[A]): List[A] = {
    @tailrec
    def f(l: List[A], h: List[A]): List[A] = l match {
      case head :: tail => f(tail, head :: h)
      case Nil => h
    }

    f(as, Nil)
  }

  def foldRightFl[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    foldLeft(reverse(as), z)(f)
  }

  def append[A](as: List[A], v: A): List[A] = {
    foldRightFl(as, List(v)) { (last, appendable) =>
      last :: appendable
    }
  }

  def addOne(as: List[Int]): List[Int] = {
    foldRightFl(as, List[Int]()) { (el, l) =>
      (el + 1) :: l
    }
  }

  def doubleToString(as: List[Double]): List[String] = {
    foldRightFl(as, List[String]()) { (el, l) =>
      el.toString :: l
    }
  }

  def map[A, B](as: List[A])(f: A => B): List[B] = {
    foldRightFl(as, List[B]()) { (el, l) =>
      f(el) :: l
    }
  }

  def filter[A](as: List[A])(f: A => Boolean): List[A] = {
    foldRightFl(as, List[A]()) { (el, l) =>
      if (f(el)) {
        el :: l
      } else {
        l
      }
    }
  }

  def flatMap[A, B](as: List[A])(f: A => List[B]): List[B] = {
    foldRightFl(as, List[B]()) { (el, l) =>
      f(el) ++ l
    }
  }

  def filterFm[A](as: List[A])(f: A => Boolean): List[A] = {
    flatMap(as)(el => if (f(el)) List(el) else Nil)
  }

  def addLists(l1: List[Int], l2: List[Int]): List[Int] = {
    @tailrec
    def f(l1: List[Int], l2: List[Int], res: List[Int]): List[Int] = (l1, l2) match {
      case (head1 :: tail1, head2 :: tail2) => f(tail1, tail2, res :+ (head1 + head2))
      case (_, _) => res
    }

    f(l1, l2, Nil)
  }

  def zipWith[A, B](l1: List[A], l2: List[A])(func: (A, A) => B): List[B] = {
    @tailrec
    def f(l1: List[A], l2: List[A], res: List[B]): List[B] = (l1, l2) match {
      case (head1 :: tail1, head2 :: tail2) => f(tail1, tail2, res :+ func(head1, head2))
      case (_, _) => res
    }

    f(l1, l2, Nil)
  }


  val res = drop(List(1, 2, 3, 4, 5), 3)
  val l = length(List(1, 2, 3, 4, 5))
  val lFl = length(List(1, 2, 3, 4, 5))
  val fl = foldLeft(List(1, 2, 3, 4, 5), 1)(_ * _)
  val fRfl = foldRightFl(List(1, 2, 3, 4, 5), 1)(_ * _)
  val r = reverse(List(1, 2, 3, 4, 5))
  val ap = append(List(1, 2, 3, 4, 5), 6)
  val aOne = addOne(List(1, 2, 3, 4, 5))
  val dTs = doubleToString(List(1.3, 2.4, 3.5, 4.0, 5.2))
  val m = map(List(1, 2, 3, 4, 5))(_ * 2)
  val even = filter(List(1, 2, 3, 4, 5, 6, 7, 8))(_ % 2 == 0)
  val fM = flatMap(List(1, 2, 3, 4, 5))(i => List(i, i))
  val ffM = filterFm(List(1, 2, 3, 4, 5, 6, 7, 8))(_ % 2 == 0)
  val adL = addLists(List(1, 2, 3, 4, 5), List(1, 2, 3, 4, 5))
  val z = zipWith(List(1, 2, 3, 4, 5), List(1, 2, 3, 4, 5))((_, _))

  println(res)
  println(l)
  println(lFl)
  println(fl)
  println(r)
  println(fRfl)
  println(ap)
  println(aOne)
  println(dTs)
  println(m)
  println(even)
  println(fM)
  println(ffM)
  println(adL)
  println(z)


}

case class Test(id: Long, name: String)

case class CsvTest(id: Long, name: String, surname: String, count: Long, mass: String)

case class CsvTestRaw(id: String, name: String, surname: String)

case class YahooStockPrice(date: String, open: Double, high: Double, low: Double, close: Double, volume: Integer, adjClose: Double)
