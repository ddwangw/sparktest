package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * 去重
 * 手动创建4个分区
 * 调用去重，打印结果
 */
public class DistinctOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> mapRDD = sc.parallelize(Arrays.asList("a", "b", "c", "d","b", "c", "d","c", "d","b") ,4);
        List<String> temp = mapRDD.distinct().collect();
        for(String t : temp){
            System.out.println(t);
        }
    }
}
