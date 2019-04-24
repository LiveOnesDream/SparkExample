package com.spark.core;

import com.spark.core.Initialize.DataInit;
import com.spark.core.dao.SparkOperator;
import org.apache.log4j.Logger;

/**
 * 算子测试类
 */

public class ApplicationStart {

    static Logger logger = Logger.getLogger("ApplicationStart");

    public static void main(String[] args) {

        logger.info("Program startting!");

        DataInit.getSparkConn();

        SparkOperator dao = new SparkOperator();
//        dao.MapOperator();
        dao.MapPartitionsOperator();


    }
}
