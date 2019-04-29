package com.ddwanglife.levelone.programming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;

public class FilterOperator {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("FilterOperator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext (conf);

        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("pandas", "i like pandas","liu de hua","guan zhi ling"));
        JavaRDD<String> filterResultRDD = stringRDD.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                if(s.contains("an")){
                    return false;
                }else{
                    return true;
                }
            }
        });
        filterResultRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
