package com.spark.core.SparkPaseJson;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.sparkDemo;


public class Operator_json {

    public static void main(String[] args) {

        SparkConf sconf = new SparkConf().setAppName("Operator_json").setMaster("local[2]").set("spark.testing.memory", "2147480000");
        SparkSession spark = SparkSession.builder().config(sconf).getOrCreate();

//        Dataset<Row> people = spark.read().json("C:\\Users\\zp244\\Desktop\\people.json");
//        people.show(100);
//        people.registerTempTable("people");
//
//        spark.sql("select name,nums[2] from people ").show();

        Dataset ds = sparkDemo.fileds(spark);
        ds.registerTempTable("test");
        spark.sql("select * from test where id <=6").show();


    }

}
