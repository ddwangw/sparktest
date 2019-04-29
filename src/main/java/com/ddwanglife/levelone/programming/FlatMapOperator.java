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

public class FlatMapOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("FlatMapOperator").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> strRDD = context.parallelize(Arrays.asList("wang dong dong","zhang ting ting","wang zu shan","qian kun quan"));
        JavaRDD<Integer> countCharLengthRDD = strRDD.flatMap(new FlatMapFunction<String, Integer>() {
            public Iterator<Integer> call(String s) throws Exception {
                List<Integer> countResult = new ArrayList<Integer>();
                for(String word : s.split(" ")){
                    countResult.add(word.length());
                }
                return countResult.iterator();
            }
        });
        countCharLengthRDD.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
