package com.ddwanglife.levelone;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class WordCount {
    public static void main(String[] args) {
//        String inputSparkFile = args[0];
        String inputSparkFile = "hdfs://bigdata40.hadoop/spark_level_one/spark.txt";
//        String outputSparkFile = args[1];
        String outputSparkFile = "hdfs://bigdata40.hadoop/spark_level_one/output/spark_count.txt";

        //创建 SparkConf对象，对程序进行必要的配置
//        SparkConf conf = new SparkConf()
//                .setAppName("WordCount").setMaster("local");
        //取消本地 默认都是集群
        SparkConf conf = new SparkConf()
                .setAppName("WordCount");

        //通过conf创建上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建初始RDD
//        JavaRDD<String> lines = sc.textFile("D://spark.txt");
        JavaRDD<String> lines = sc.textFile(inputSparkFile);

        //----用各种Transformation算子对RDD进行操作-----------------------------------------
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //----用一个 action 算子触发job-----------------------------------------
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                // TODO Auto-generated method stub
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times");
            }
        });
        wordCounts.saveAsTextFile(outputSparkFile);
//        wordCounts.saveAsTextFile("D://resultCount");
    }
}
