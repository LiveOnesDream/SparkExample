package com.spark.core.SparkPaseJson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;


public class Operator_json01 {

    public static void main(String[] args) {
        // TODO 自动生成的方法存根
        SparkConf conf = new SparkConf().setMaster("local").setAppName("MyMp3");
        conf.set("spark.testing.memory", "2147480000");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile("C:\\Users\\zp244\\Desktop\\JSON.json");
        JavaRDD<Mp3Info> result = input.mapPartitions(new ParseJson()).
                filter(
                        x -> x.getAlbum().equals("怀旧专辑")
                );

//        JavaRDD<Mp3Info> r = input.mapPartitions(new ParseJson()).filter(y -> y.getName().equals("上海滩"));

        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());

        result.foreach(x -> System.out.println(x));
        formatted.saveAsTextFile("C:\\Users\\zp244\\Desktop\\json_test.json");

        jsc.close();

    }

}

class WriteJson implements FlatMapFunction<Iterator<Mp3Info>, String> {

    private static final long serialVersionUID = -6590868830029412793L;

    public Iterator<String> call(Iterator<Mp3Info> song) throws Exception {
        ArrayList<String> text = new ArrayList<String>();
        ObjectMapper mapper = new ObjectMapper();
        while (song.hasNext()) {
            Mp3Info person = song.next();
            text.add(mapper.writeValueAsString(person));
        }
        return text.iterator();
    }
}


class Mp3Info implements Serializable {

    private static final long serialVersionUID = -3811808269846588364L;
    private String name;
    private String album;
    private String path;
    private String singer;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlbum() {
        return album;
    }

    public void setAlbum(String album) {
        this.album = album;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSinger() {
        return singer;
    }

    public void setSinger(String singer) {
        this.singer = singer;
    }

    @Override
    public String toString() {
        return "Mp3Info [name=" + name + ", album="
                + album + ", path=" + path + ", singer=" + singer + "]";
    }
}

class ParseJson implements FlatMapFunction<Iterator<String>, Mp3Info> {

    private static final long serialVersionUID = 8603650874403773926L;

    @Override
    public Iterator<Mp3Info> call(Iterator<String> lines) throws Exception {
        // TODO 自动生成的方法存根
        ArrayList<Mp3Info> mp3 = new ArrayList<Mp3Info>();
        ObjectMapper mapper = new ObjectMapper();
        while (lines.hasNext()) {
            String line = lines.next();
            try {
                mp3.add(mapper.readValue(line, Mp3Info.class));
            } catch (Exception e) {

            }
        }
        return mp3.iterator();
    }

}





