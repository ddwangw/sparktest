package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;


public class MapOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf()
                .setAppName("MapOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 3, 5, 7),4);
        JavaRDD<Integer> intMapRDD = intRDD.map( new Function<Integer,Integer>(){
            private static final long serialVersionUID = 1L;
            public Integer call(Integer integer) throws Exception {
                return integer*10;
            }
        });
        intMapRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        sc.close();
    }
}
