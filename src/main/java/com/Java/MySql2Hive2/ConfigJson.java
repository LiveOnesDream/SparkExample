package com.Java.MySql2Hive2;

import com.Java.MySql2Hive.CreateJsonFile;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 批量获取数据下所有表并批量配置DataX 任务json文件
 */

public class ConfigJson {

    public static Connection con = null;
    public static PreparedStatement ps = null;
    public static DatabaseMetaData meta = null;
    public static ResultSet rs = null;
    public static ResultSetMetaData rmeta = null;

    /**
     * 得到指定库下所有表
     *
     * @param URL
     * @param USERNAME
     * @param PASSWORD
     * @return
     */

    public static List<String> getTableInfo(String URL, String USERNAME, String PASSWORD) {

        List<String> tables = new ArrayList<String>();

        try {

            //1.连接mysql数据库
            con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            Class.forName("com.mysql.jdbc.Driver");
            meta = con.getMetaData();
            //2.得到数据库下所有表
            rs = meta.getTables(null, null, null, new String[]{"TABLE"});

            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return tables;
    }

    /**
     * 批量配置json 文件
     *
     * @param url    数据库连接
     * @param use    用户
     * @param passwd 密码
     * @throws SQLException
     * @throws IOException
     */
    public static void batchConfig(String url, String use, String passwd) throws SQLException, IOException {

        List<String> tables = getTableInfo(url, use, passwd);
        List<String> columns = new ArrayList<>();
        CreateJsonFile jsonFile = new CreateJsonFile();

        int tableCount;
        for (String tableName : tables) {

            jsonFile.createjosnInfo1("C:\\Users\\Administrator\\Desktop\\json\\" + tableName + ".json");
            jsonFile.createjosnInfo2(use, passwd);
            jsonFile.createjosnInfo3(tableName);
            jsonFile.createjosnInfo4(url);
            jsonFile.createjosnInfo5();

            String sql = "select * from " + tableName;
            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();
            rmeta = rs.getMetaData();

            tableCount = rmeta.getColumnCount();
            System.out.println("表 " + tableName + "共有 " + tableCount + "个字段.");

            for (int i = 1; i < tableCount + 1; i++) {

                String str = "                              {\n";
                String name = "                                     \"name\"" + ":" + "\"" + rmeta.getColumnName(i) + "\"," + "\n";
                String type = "                                     \"type\"" + ":" + "\"string\"," + "\n";
                String str1 = "                              },\n";

                jsonFile.createjosnInfo6(str, name, type);

                // 判断是否最后一个列名，是则将都逗号删了。
                if (i != tableCount) {
                    jsonFile.createjosnInfo6(str1);
                } else {
                    String lastStr1 = str1.replace("},\n", "}\n");
                    jsonFile.createjosnInfo6(lastStr1);
                }

                columns.add(rmeta.getColumnName(i));
                System.out.println(rmeta.getColumnName(i));
            }
            jsonFile.createjosnInfo7();
        }
    }

    /**
     * 测试类
     *
     * @param args
     */
    public static void main(String[] args) {

        String path = "C:\\Users\\Administrator\\Desktop\\json\\";
        String URL = "jdbc:mysql://localhost:3306/jdyp_depot";
        String USERNAME = "root";
        String PASSWORD = "123456";

        try {

            batchConfig(URL, USERNAME, PASSWORD);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


