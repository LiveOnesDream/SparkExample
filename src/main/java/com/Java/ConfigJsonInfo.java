package com.Java;


import com.Java.MySql2Hive2.MySqlInfo2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ConfigJsonInfo {

    public static String tbName = "bill_details";
    public static final String jsonPath = "C:\\Users\\Administrator\\Desktop\\"+tbName+".json";
    public static final String URL = "jdbc:mysql://localhost:3306/jdyp_topic?useUnicode=true&characterEncoding=utf-8";
    public static final String[] names = {"java", "java", "java", "hadoop", "hadoop"};


    public static void writeJson(String URL, String tbName, String USER, String PASSWD, String jsonPath) {

        FileWriter fw = null;
        BufferedWriter bw = null;

        try {

            fw = new FileWriter(jsonPath, true);
            bw = new BufferedWriter(fw);

            bw.write("{\n");
            bw.write("    " + "\"job\"" + ": {\n");
            bw.write("        " + "\"setting\"" + ": {\n");
            bw.write("            " + "\"speed\"" + ": {\n");
            bw.write("                " + "\"channel\"" + ": 4\n");
            bw.write("            }\n");
            bw.write("        },\n");
            bw.write("        " + "\"content\"" + ": [\n");
            bw.write("            {\n");
            bw.write("                " + "\"reader\"" + ":{\n ");
            bw.write("                    " + " \"name\"" + ":" + "\"mysqlreader\"" + "," + "\n");
            bw.write("                    " + " \"parameter\"" + ": {\n");
            bw.write("                        " + " \"username\"" + ":" +"\"" + USER + "\""+ ",\n");
            bw.write("                        " + " \"password\"" + ":" +"\"" + PASSWD + "\""+ ",\n");
            bw.write("                        " + " \"connection\"" + ":" + "[{\n");
            bw.write("                            " + "\"querySql\"" + ":" + "[\n");
            bw.write("                                     \"select * from " + tbName + ";\"\n");
            bw.write("                        ],\n");
            bw.write("                        " + "\" jdbcUrl\"" + ": [\n");
            bw.write("                                " + " \"jdbc\"" + ": \n");
            bw.write("                                     " + "\"" + URL + "\"" + "\n");
            bw.write("                                  ]\n");
            bw.write("                             }]\n");
            bw.write("                        }\n");
            bw.write("                },\n");
            bw.write("                " + " \"writer\"" + ": {\n");
            bw.write("                    " + " \"name\"" + ":" + "\"hdfswriter\"" + ",\n");
            bw.write("                    " + " \"parameter\"" + ": {\n");
            bw.write("                        " + " \"defaultFS\"" + ":" + "\"hdfs://sinan3:8020\"" + ",\n");
            bw.write("                        " + "\" fileType\"" + ":" + "\"text\" " + ",\n");
            bw.write("                        " + " \"path\"" + ":" + "\"pt = $ { bizdate } 000000\" " + ",\n");
            bw.write("                        " + " \"column\"" + ": [\n");

            List<String> names = MySqlInfo2.getColumnName();
            int i = 0;
            for (String key : names) {

                String str = "                              {\n";
                String name = "                                     \"name\"" + ":" + "\"" + key + "\"," + "\n";
                String type = "                                     \"type\"" + ":" + "\"string\"," + "\n";
                String str1 = "                              },\n";

                bw.write(str);
                bw.write(name);
                bw.write(type);

                if (i != names.size() - 1) {
                    bw.write(str1);
                } else {
                    String lastStr1 = str1.replace("},\n", "}\n");
                    bw.write(lastStr1);
                }
                i++;
            }

            bw.write("                        ],\n");
            bw.write("                        " + " \"writeMode\"" + ":" + "\"append\"," + "\n");
            bw.write("                        " + " \"fieldDelimiter\"" + ":" + " \" \\t \"" + "\n");
            bw.write("                    }\n");
            bw.write("                }\n");
            bw.write("            }\n");
            bw.write("        ]\n");
            bw.write("    }\n");
            bw.write("}\n");

            System.out.println("json文件配置成功");

            bw.flush();
            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 列名配置  测试
     */
    public static void configColumn() {

        String[] names = {"java", "java", "java", "hadoop", "hadoop"};

        int i = 0;
        for (String key : names) {
            String str = "{\n";
            String name = "          \"name\"" + ":" + "\"" + key + "\"," + "\n";
            String type = "          \"type\"" + ":" + "\"string\"," + "\n";
            String str1 = "},\n";

            System.out.println(str);
            System.out.println(name);
            System.out.println(type);

            if (i != names.length - 1) {
                System.out.println(str1);
            } else {
                String lastStr1 = str1.replace("},\n", "}\n");
                System.out.println(lastStr1);
            }
            i++;
        }

    }

    /**
     * 测试类
     */
    public static void main(String[] args) {

//        writeJson(jsonPath, tbName, URL);
        writeJson(URL, tbName, "root", "123456", jsonPath);

    }
}
