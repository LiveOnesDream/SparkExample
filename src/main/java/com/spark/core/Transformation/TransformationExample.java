package com.spark.core.Transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.internal.Trees;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Transformation 算子操作
 */
public class TransformationExample {

    public static SparkConf sconf = new SparkConf().setAppName("Transformation_Op").setMaster("local[4]");
    public static JavaSparkContext sc = new JavaSparkContext(sconf);
    public static SparkSession sparkSession = SparkSession.builder().getOrCreate();


    public static final List<String> list = Arrays.asList("张无忌", "赵敏", "周芷若");


    public static void map() {

        //通过并行化的方式创建RDD
        final JavaRDD<String> rdd = sc.parallelize(list);

        JavaRDD<String> name = rdd.map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {

                return "hello" + name;
            }
        });

        action(name);

    }


    /**
     * flatmap()
     */
    public static void flatMap() {

//        SparkConf sconf = new SparkConf().setAppName("Transformation_Op").setMaster("local[2]");
//        SparkContext sc = new SparkContext(sconf);
//        SQLContext sqlContext = new SQLContext(sc);
//        int[] i = {1,2,3,4,5,6,7,8};
//        JavaRDD<Integer> numrdd = sparkSession.mk

        JavaRDD<String> rdd = sc.parallelize(list);
        JavaRDD<String> name = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String name) throws Exception {

                return Arrays.asList(name.split(" ")).iterator();
            }
        }).map(new Function<String, String>() {
            @Override
            public String call(String name) throws Exception {
                return "hello" + name;
            }
        });

        action(name);
    }


    public static void filterExample() {

        final List list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> filterrdd = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 1;
            }
        });

        filterrdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }


    public static void groupBykeyExample() {

        final List list = Arrays.asList(
                new Tuple2("峨眉", "周芷若"),
                new Tuple2("武当", "宋青书"),
                new Tuple2("峨眉", "灭绝师太"),
                new Tuple2("武当", "张三丰"));
        JavaPairRDD rdd = sc.parallelizePairs(list);
        JavaRDD rdd1 = sc.parallelize(list);

        JavaPairRDD groupByKeyRDD = rdd.groupByKey();

        groupByKeyRDD.foreach(new VoidFunction() {
            @Override
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });
    }

    public static void reduceByKeyExapmle() {

        final List<String> list = Arrays.asList("zhangsan", "zhangsan", "pandas", "numpy", "pip", "pip", "pip");

        JavaRDD<String> rdd = sc.parallelize(list);
        JavaPairRDD<String, Integer> reduceByKeyRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> resultRDD = reduceByKeyRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        });

        int size = resultRDD.partitions().size();
        System.out.println("resultRDD.partitions().size();   " + size);

        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

    }

    public static void CombineByKeyExapmle_1() {

        List<Tuple2<String, Integer>> users = new ArrayList<Tuple2<String, Integer>>();
        Tuple2<String, Integer> user1 = new Tuple2<String, Integer>("1212", 1);
        Tuple2<String, Integer> user2 = new Tuple2<String, Integer>("1213", 3);
        Tuple2<String, Integer> user3 = new Tuple2<String, Integer>("1214", 6);
        Tuple2<String, Integer> user4 = new Tuple2<String, Integer>("1215", 3);
        Tuple2<String, Integer> user5 = new Tuple2<String, Integer>("1212", 1);
        users.add(user1);
        users.add(user2);
        users.add(user3);
        users.add(user4);
        users.add(user5);

        JavaPairRDD<String, Integer> kvJavaPairRDD = sc.parallelizePairs(users);
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairRDD = kvJavaPairRDD.combineByKey(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        }, new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Integer x) throws Exception {
                return new Tuple2<>(v1._1() + x, v1._2() + x);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                return new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2());
            }
        });

        pairRDD.foreach(s -> System.out.println(s));
    }

    public static void CombineByKeyExapmle_2() {

        List<String> l1 = new ArrayList<>();
        l1.add("dog");
        l1.add("cat");
        l1.add("gnu");
        l1.add("salmon");
        l1.add("rabbit");
        l1.add("turkey");
        l1.add("wolf");
        l1.add("bear");
        l1.add("bee");

        JavaRDD<String> javaRDD = sc.parallelize(l1, 3);
        JavaRDD<Integer> javaRDD2 = sc.parallelize(Arrays.asList(1, 1, 2, 3, 2, 1, 3, 2, 2), 3);

        JavaPairRDD<Integer, String> javaPairRDD = javaRDD2.zip(javaRDD);
        JavaPairRDD<Integer, List<String>> javaPairRDD2 = javaPairRDD
                //输入string，返回List<String>,也就是将每个partition的第一个元素(String类型)添加到list中，
                // 此时每个partition中的元素为List<string>,string,string
                .combineByKey(new Function<String, List<String>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public List<String> call(String arg0) throws Exception {
                        List<String> list = new ArrayList<>();
                        list.add(arg0);
                        return list;
                    }
                }, new Function2<List<String>, String, List<String>>() {
                    private static final long serialVersionUID = 1L;

                    //输入List<String>和String，这里的List<String>就是上一个函数作用的结果,
                    // 这一步作用是把每个partition中剩余的String类型元素添加到List<String>当中，最后返回一个List<String>
                    @Override
                    public List<String> call(List<String> arg0, String arg1) throws Exception {
                        arg0.add(arg1);
                        return arg0;
                    }
                }, new Function2<List<String>, List<String>, List<String>>() {
                    private static final long serialVersionUID = 1L;

                    //输入List<String>,输出List<String>,这一个函数作用是把各个partition中的List<String>进行合并，返回最终的List<String>
                    @Override
                    public List<String> call(List<String> arg0, List<String> arg1) throws Exception {
                        arg0.addAll(arg1);
                        return arg0;
                    }
                });

        javaPairRDD2.foreach(x -> System.out.print(x + " "));

    }

    /**
     * 算子测试类
     *
     * @param args
     */
    public static void main(String[] args) {

//        map();
//        flatMap();
//        filterExample();
//        groupBykeyExample();
//        mapValuesExample();
//        reduceByKeyExapmle();
//        CombineByKeyExapmle_1();
        CombineByKeyExapmle_2();
    }


    public static void action(JavaRDD<String> rdd) {

        rdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });
    }
}
