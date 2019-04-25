package com.ddwanglife.levelone;

import java.util.Arrays;
import java.util.Iterator;

import com.ddwanglife.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;


public class WordCount {
    public static void main(String[] args) {
//        String inputSparkFile = args[0];
//        String inputSparkFile = "hdfs://bigdata40.hadoop/spark_level_one/spark.txt";
//        String outputSparkFile = args[1];
//        String outputSparkFile = "hdfs://bigdata40.hadoop/spark_level_one/output/spark_count.txt";

        //创建 SparkConf对象，对程序进行必要的配置
        SparkConf conf = new SparkConf()
                .setAppName("WordCount").setMaster("local");
        //取消本地 默认都是集群
//        SparkConf conf = new SparkConf()
//                .setAppName("WordCount");

        //通过conf创建上下文对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建初始RDD
        JavaRDD<String> lines = sc.textFile("D://spark.txt");
//        JavaRDD<String> lines = sc.textFile(inputSparkFile);

        //----用各种Transformation算子对RDD进行操作-----------------------------------------
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(String line) throws Exception {
                line = line.replace(",","");
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
        JavaPairRDD<Integer,String> wordCountsSortByValue =  wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                // 交换key value的顺序
                return new Tuple2<Integer, String>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        }).sortByKey(false);
        //----用一个 action 算子触发job-----------------------------------------
        wordCountsSortByValue.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            public void call(Tuple2<Integer, String> wordCount) throws Exception {
                String insertSqlStr = "INSERT INTO word_count_result(word,count,create_time) VALUES (?, ?, SYSDATE());";
                Object[] obj = {wordCount._2,wordCount._1};
                JDBCHelper  insertSql = JDBCHelper.getInstance();
                insertSql.executeUpdate(insertSqlStr,obj);
            }
        });
//        wordCounts.saveAsTextFile(outputSparkFile);
//        wordCounts.saveAsTextFile("D://resultCount");
        sc.close();
    }
}
