package com.spark.core.Initialize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataInit {

    private static final Logger looger = LoggerFactory.getLogger(DataInit.class);

    public static SparkConf sconf;
    public static SparkSession sparkSession;
    public static JavaSparkContext sc;

    public static void getSparkConn() {
        sconf = new SparkConf()
                .setAppName("DataInit")
                .setMaster("local");
        sconf.set("spark.cores.max", "2");
        sparkSession = SparkSession.builder().config(sconf).getOrCreate();
        sc = new JavaSparkContext(sparkSession.sparkContext());
        System.setProperty("hadoop.home.dir", "F:/hadoop-eclipse-plugin");

    }
}
