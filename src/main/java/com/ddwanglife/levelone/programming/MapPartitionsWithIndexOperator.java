package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;

public class MapPartitionsWithIndexOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> mapRDD = sc.parallelize(Arrays.asList("a","b","c","d"),2);
        JavaRDD<String> resultRDD = mapRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                System.out.println(integer);
                return stringIterator;
            }
        },true);
        resultRDD.collect();
        sc.close();
    }

}
