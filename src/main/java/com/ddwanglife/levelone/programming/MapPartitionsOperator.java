package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 创建有两个partition的 RDD
 * 进行mapPartition 的时候，传进一个函数，对每个partition进行运算，返回一个javaRDD
 *
 * 例如 1 2 3 4 5 分为两个partition 1  2  和  3  4  5
 * 计算结果  3  和  12
 */
public class MapPartitionsOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("MapPartitionsOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> initStrRDD = sc.parallelize(Arrays.asList(1,2,3,4,5),2);
        JavaRDD<Integer> mapParRDD = initStrRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            public Iterator<Integer> call(Iterator<Integer> iterator) throws Exception {
                Integer sumNum = 0;
                while(iterator.hasNext()){
                    sumNum += iterator.next();
                }
                return Arrays.asList(sumNum).iterator();
            }
        });
        mapParRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
