package com.Java.MySql2Hive;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateJsonFile {

    public static FileWriter writer;

    public void createjosnInfo1(String path) throws IOException {

        writer = new FileWriter(path, true);

        writer.write("{\n");
        writer.write("    " + "\"job\"" + ": {\n");
        writer.write("        " + "\"setting\"" + ": {\n");
        writer.write("            " + "\"speed\"" + ": {\n");
        writer.write("                " + "\"channel\"" + ": 4\n");
        writer.write("            }\n");
        writer.write("        },\n");
        writer.write("        " + "\"content\"" + ": [\n");
        writer.write("            {\n");
        writer.write("                " + "\"reader\"" + ":{\n ");
        writer.write("                    " + " \"name\"" + ":" + "\"mysqlreader\"" + "," + "\n");
        writer.write("                    " + " \"parameter\"" + ": {\n");

    }

    public void createjosnInfo2(String USER, String PASSWD) throws IOException {

        writer.write("                        " + " \"username\"" + ":" + "\"" + USER + "\"" + ",\n");
        writer.write("                        " + " \"password\"" + ":" + "\"" + PASSWD + "\"" + ",\n");
        writer.write("                        " + " \"connection\"" + ":" + "[{\n");
        writer.write("                            " + "\"querySql\"" + ":" + "[\n");
    }

    public void createjosnInfo3(String tbName) throws IOException {

        writer.write("                                     \"select * from " + tbName + ";\"\n");
        writer.write("                        ],\n");
        writer.write("                        " + "\" jdbcUrl\"" + ": [\n");
        writer.write("                                " + " \"jdbc\"" + ": \n");
    }

    public void createjosnInfo4(String URL) throws IOException {

        writer.write("                                     " + "\"" + URL + "\"" + "\n");
        writer.write("                                  ]\n");
        writer.write("                             }]\n");
        writer.write("                        }\n");
        writer.write("                },\n");
    }

    public void createjosnInfo5() throws IOException {

        writer.write("                " + " \"writer\"" + ": {\n");
        writer.write("                    " + " \"name\"" + ":" + "\"hdfswriter\"" + ",\n");
        writer.write("                    " + " \"parameter\"" + ": {\n");
        writer.write("                        " + " \"defaultFS\"" + ":" + "\"hdfs://sinan3:8020\"" + ",\n");
        writer.write("                        " + "\" fileType\"" + ":" + "\"text\" " + ",\n");
        writer.write("                        " + " \"path\"" + ":" + "\"pt = $ { bizdate } 000000\" " + ",\n");
        writer.write("                        " + " \"column\"" + ": [\n");
    }

    public void createjosnInfo6(List<String> columns) throws IOException {

        int i = 0;
        for (String key : columns) {

            String str = "                              {\n";
            String name = "                                     \"name\"" + ":" + "\"" + key + "\"," + "\n";
            String type = "                                     \"type\"" + ":" + "\"string\"," + "\n";
            String str1 = "                              },\n";

            writer.write(str);
            writer.write(name);
            writer.write(type);

            if (i != columns.size() - 1) {
                writer.write(str1);
            } else {
                String lastStr1 = str1.replace("},\n", "}\n");
                writer.write(lastStr1);
            }
            i++;
        }
    }
    public void createjosnInfo6(String str,String name,String type) throws IOException {

        writer.write(str);
        writer.write(name);
        writer.write(type);

    }
    public void createjosnInfo6(String str1) throws IOException {
        writer.write(str1);
    }

    public void createjosnInfo7() throws IOException {

        writer.write("                        ],\n");
        writer.write("                        " + " \"writeMode\"" + ":" + "\"append\"," + "\n");
        writer.write("                        " + " \"fieldDelimiter\"" + ":" + " \" \\t \"" + "\n");
        writer.write("                    }\n");
        writer.write("                }\n");
        writer.write("            }\n");
        writer.write("        ]\n");
        writer.write("    }\n");
        writer.write("}\n");

        writer.flush();
        writer.close();

    }

    public static void main(String[] args) {

        CreateJsonFile jsonFile = new CreateJsonFile();

        List<String> list = new ArrayList<>();
        list.add("java");
        list.add("hadoop");
        list.add("php");

        try {
            jsonFile.createjosnInfo1("C:\\Users\\Administrator\\Desktop\\" + "sss" + ".json");
            jsonFile.createjosnInfo2("zhangsan", "wangwu");
            jsonFile.createjosnInfo3("table");
            jsonFile.createjosnInfo4("jdbc:mysql://localhost:3306/jdyp_topic");
            jsonFile.createjosnInfo5();
            jsonFile.createjosnInfo6(list);
            jsonFile.createjosnInfo7();

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
