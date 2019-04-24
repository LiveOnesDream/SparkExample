package com.spark.core.dao;

import com.spark.core.Initialize.DataInit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 各种算子RDD操作
 */

public class SparkOperator implements scala.Serializable {

    private static Logger logger = LoggerFactory.getLogger(SparkOperator.class);

    /**
     * 算子:map
     * 通过函数将RDD中的每个元素进行转换形成一个新的RDD。
     */

    public void MapOperator() {

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD RDD = DataInit.sc.parallelize(numbers);
        JavaRDD result = RDD.map(new Function() {
            @Override
            public Object call(Object v1) throws Exception {
                return "number:" + v1 + 1;
            }
        });

        result.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }

    /**
     * 算子:MapPartitions
     * 作用于map一致，不过是以每个parition作为一个操作单位的，所以返回类型是一个Iterator。
     */
    public void MapPartitionsOperator() {
        List<String> names = Arrays.asList("zhangsan", "lisi", "wangwu");

        JavaRDD nameRDD = DataInit.sc.parallelize(names,10);

        final Map<String, Integer> scoreMap = new HashMap<>();
        scoreMap.put("zhangsan", 100);
        scoreMap.put("lisi", 90);
        scoreMap.put("wangwu", 98);

        JavaRDD<Integer> resulte = nameRDD.mapPartitions(new FlatMapFunction() {
            @Override
            public Iterator call(Object o) throws Exception {
                List<Integer> scores = new ArrayList<>();
                Iterator it = names.iterator();
                while (it.hasNext()) {
                    String name = (String) it.next();
                    int score = scoreMap.get(name);
                    scores.add(score);
                }
                return scores.iterator();
            }
        });

        resulte.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }


}
