package hw1;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("word count").setMaster("local[4]");

        JavaSparkContext spark = new JavaSparkContext(conf);


        JavaRDD<String> lines =
                spark.textFile("/Users/gchemiakin/Documents/Projects/hw1_i/src/main/resources/input", 1);


        JavaRDD<String> words = lines.flatMap(line -> {
            List<String> strings = Arrays.asList(line.split(" "));
            return strings.iterator();
        });

        JavaPairRDD<String, Integer> pair = words.mapToPair(w -> new Tuple2<>(w, 1));

        JavaPairRDD<String, Integer> integerJavaPairRDD = pair.reduceByKey(Integer::sum).cache();

        List<Tuple2<String, Integer>> collect = integerJavaPairRDD.collect();

        System.out.println(collect);

        Tuple2<String, Integer> res = integerJavaPairRDD.reduce((a, b) -> a._2 > b._2 ? a : b);

        System.out.println(res);
    }
}

