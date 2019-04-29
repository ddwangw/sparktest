package com.ddwanglife.levelone;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import com.google.common.collect.Lists;
public class KafkaStreamingWordCount {

    public static void main(String[] args) throws Exception{
        //设置匹配模式，以空格分隔
        final Pattern SPACE = Pattern.compile(" ");
        //接收数据的地址和端口
        String zkQuorum = "localhost:2181";
        //话题所在的组
        String group = "1";
        //话题名称以“，”分隔
        String topics = "top_test";
        //每个话题的分片数
        int numThreads = 2;
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
//        jssc.checkpoint("checkpoint"); //设置检查点
        //存放话题跟分片的映射关系
        Map<String, Integer> topicmap = new HashMap<String, Integer>();
        String[] topicsArr = topics.split(",");
        int n = topicsArr.length;
        for(int i=0;i<n;i++){
            topicmap.put(topicsArr[i], numThreads);
        }
        //从Kafka中获取数据转换成RDD
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap);
        //从话题中过滤所需数据
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(Tuple2<String, String> arg0)
                    throws Exception {
                return Lists.newArrayList(SPACE.split(arg0._2)).iterator();
            }
        });
        //对其中的单词进行统计
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        //打印结果
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}