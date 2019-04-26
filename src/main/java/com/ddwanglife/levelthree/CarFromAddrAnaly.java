package com.ddwanglife.levelthree;

import com.ddwanglife.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 卡扣通过车辆来源地分析
 * 通过识别的车牌，分析车辆是从哪个省份来的，做好数据分析
 *
 */

public class CarFromAddrAnaly {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("DayFlowCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lineStr = sc.textFile("E:\\\\jtkk\\\\jtkk.txt");
        //过滤,预计返回结果是不包括00000000的异常数据
        JavaRDD<String> noHaveErrorCph = lineStr.filter(new Function<String,Boolean>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(String s) throws Exception {
                String[] list = s.split("\t");
                if("00000000".equals(list[4])) {
                    return false;
                }
                return true;
            }
        });
        //获取车牌号并去重
        JavaRDD<String> noRepeatChp = noHaveErrorCph.map(new Function<String,String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String s) throws Exception {
                String[] list = s.split("\t");
                return list[4];
            }
        }).distinct();
        //过滤,预计返回结果是不包括00000000的异常数据
        JavaPairRDD<String, Integer> pairs = noRepeatChp.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                //获取车牌号所属省
                String proSimple =  s.substring(0,1);
                //保留第一个字符
                return new Tuple2(proSimple,1);
            }
        });
        //统计所有省的集合
        JavaPairRDD<String, Integer> proviceCarCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        //打印数据，入库
        List<Tuple2<String, Integer>> listToString =  proviceCarCount.collect();
        for(Tuple2<String, Integer> tempStr : listToString){
//           System.out.println(tempStr._1+"   "+tempStr._2);
// pro_simple 省简写  count_num  统计的车辆数（车辆车牌通过记录已去重）
            String insertSqlStr = "INSERT INTO traffic_pro_car_count(pro_simple,count_num,create_time) VALUES (?, ?, SYSDATE());";
            Object[] obj = {tempStr._1,tempStr._2};
            JDBCHelper insertSql = JDBCHelper.getInstance();
            insertSql.executeUpdate(insertSqlStr,obj);
        }
    }
}
