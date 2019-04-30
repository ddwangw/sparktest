package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 *  返回一个RDD，它将每个分区中的所有元素合并到一个列表中
 *
 *  与其说是一个列表，不如说是一个集合
 *
 */
public class GlomOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("GlomOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> glomRDD = sc.parallelize(Arrays.asList("a","b","c","d","e"),2);
        //当parallelize 使用默认1个partition时，打印一次  Glom 返回结果集list大小 ： 5
        //参数为2的时候打印两次，Glom 返回结果集list大小 ： 2   Glom 返回结果集list大小 ： 3
        JavaRDD<List<String>> glomRDDResult = glomRDD.glom();
        glomRDDResult.foreach(new VoidFunction<List<String>>() {
            public void call(List<String> strings) throws Exception {
                System.out.println("Glom 返回结果集list大小 ： "+strings.size());
            }
        });
    }
}
