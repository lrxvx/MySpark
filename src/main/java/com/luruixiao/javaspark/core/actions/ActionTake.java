package com.luruixiao.javaspark.core.actions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author luruixiao
 */
public class ActionTake {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("./data/words");
        List<String> resultList = lines.take(3);
        for(String one:resultList){
            System.out.println(one);
        }
    }
}
