package com.spark.SparkSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;

public class DataSetOperator {

    public static SparkSession session = SparkSession.builder().master("local[2]").appName("DataSetOperator").getOrCreate();
    final JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());

    public static void flatMap_Example() {

        JavaRDD<String> rdd1 = session.read().textFile("C:\\Users\\Administrator\\Desktop\\Wordcount.txt").toJavaRDD();

        JavaRDD<Row> map = rdd1.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {

                String[] split = line.split(" ");
                String filed1 = split[0];
                String filed2 = split[1];

                return RowFactory.create(filed1, filed2);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("filed1", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("filed2", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = session.createDataFrame(map, schema);

        df.show(1000);

    }

    public static void main(String[] args) {
        flatMap_Example();

    }
}
