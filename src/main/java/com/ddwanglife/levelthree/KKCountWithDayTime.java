package com.ddwanglife.levelthree;

import com.ddwanglife.jdbc.JDBCHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 按天统计车流量
 * id
 * key 2018-04-14
 * count_number 12220
 */
public class KKCountWithDayTime {
    private static final String janPattern = "(0?[13578]|1[02])/(0?[1-9]|[12][0-9]|3[01])";
    private static final String febPattern = "0?2/(0?[1-9]|[12][0-9])";
    private static final String aprPattern = "(0?[469]|11)/(0?[1-9]|[12][0-9]|30)";
    private static final String timeFormat = String.format("^2[0-9]{3}/(%s|%s|%s) ([01][0-9]|2[0-3])(:[0-5][0-9]){2}$", febPattern, janPattern, aprPattern);


    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf().setAppName("DayFlowCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lineStr = sc.textFile("E:\\\\jtkk\\\\jtkk.txt");

        //过滤,预计返回结果是不包括00000000的异常数据
       JavaRDD<String> canUseLineStr = lineStr.filter(new Function<String,Boolean>(){
           private static final long serialVersionUID = 1L;
            @Override
            public Boolean call(String s) throws Exception {
                String[] list = s.split("\t");
               if("00000000".equals(list[4])||!list[7].matches(timeFormat)) {
                   return false;
               }
                return true;
            }
        });
       //按分钟统计流量
        //注意，一分钟之内出现的相同车牌没做过滤
        /*JavaRDD<String> minCount = canUseLineStr.map(new Function<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String s) throws Exception {
                String timeStr =  s.split("\t")[7];
                String dayTime = timeStr.split(" ")[0];
                String time = timeStr.split(" ")[1].substring(0,2);
                return dayTime+" "+time;
            }
        });*/
        JavaPairRDD<Date, Integer> pairs = canUseLineStr.mapToPair(new PairFunction<String, Date, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Date, Integer> call(String s) throws Exception {
                String timeStr =  s.split("\t")[7];
                String dayTime = timeStr.split(" ")[0];
//                String time = timeStr.split(" ")[1].substring(0,2);
//                return new Tuple2<String, Integer>(dayTime+" "+time,1);
                SimpleDateFormat sDateFormat=new SimpleDateFormat("yyyy/MM/dd");
                return new Tuple2<Date, Integer>(sDateFormat.parse(dayTime),1);
            }
        });
        JavaPairRDD<Date, Integer> flowCount =  pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).sortByKey();
       List<Tuple2<Date, Integer>> listToString =  flowCount.collect();
        SimpleDateFormat sDateFormat=new SimpleDateFormat("yyyy-MM-dd");
       for(Tuple2<Date, Integer> tempStr : listToString){
           String insertSqlStr = "INSERT INTO traffic_day_flow_count(cross_time,flow_car_num,create_time) VALUES (?, ?, SYSDATE());";
           Object[] obj = {sDateFormat.format(tempStr._1),tempStr._2};
           JDBCHelper insertSql = JDBCHelper.getInstance();
           insertSql.executeUpdate(insertSqlStr,obj);
       }
    }
}
