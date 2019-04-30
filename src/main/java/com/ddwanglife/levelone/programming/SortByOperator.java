package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * 排序
 */
public class SortByOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortByOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> mapRDD = sc.parallelize(Arrays.asList("1-wangdongdong","4-zhangtingting","3-wangzushan","2-qiankunquan"));
        List<String> temp = mapRDD.distinct().collect();
        for(String t : temp){
            System.out.println(t);
        }
    }
}
