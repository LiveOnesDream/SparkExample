package com.Java.MySql2Hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfigJson {

    public static String path = "C:\\Users\\Administrator\\Desktop\\" + "sss" + ".json";

    public static void batchConfig(Map<String,String> URLmap ) throws IOException {

        CreateJsonFile jsonFile = new CreateJsonFile();

        List<String> tables = new ArrayList<String>();
        List<String> columns = new ArrayList<>();

        for (String key : URLmap.keySet()) {

            String[] userAndPasswd = URLmap.get(key).split("=");
            jsonFile.createjosnInfo1(path);
            jsonFile.createjosnInfo2(userAndPasswd[0],userAndPasswd[1]);

            jsonFile.createjosnInfo3("test");

            jsonFile.createjosnInfo4(key);
            jsonFile.createjosnInfo5();
            jsonFile.createjosnInfo6(columns);
            jsonFile.createjosnInfo7();

        }
    }
}
