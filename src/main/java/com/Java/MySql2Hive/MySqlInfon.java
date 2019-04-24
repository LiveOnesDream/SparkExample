package com.Java.MySql2Hive;

import java.sql.*;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;


public class MySqlInfon {

    public static String URL = null;
    public static String use = null;
    public static String passwd = null;
    private final static String DRIVER = "com.mysql.jdbc.Driver";

    public static Connection con = null;
    public static PreparedStatement ps = null;
    public static ResultSetMetaData meta = null;
    public static DatabaseMetaData dbMetaData = null;
    public static ResultSet rs = null;


    public static void dataBaseInfo(Map<String, String> URLmap) throws SQLException, ClassNotFoundException {

        List<String> tables = new ArrayList<String>();
        List<String> columns = new ArrayList<>();

        for (String key : URLmap.keySet()) {

            String[] userAndPasswd = URLmap.get(key).split("=");
            con = DriverManager.getConnection(key, userAndPasswd[0], userAndPasswd[1]);
            Class.forName(DRIVER);
            dbMetaData = con.getMetaData();

            //1.得到数据库下所有表
            rs = dbMetaData.getTables(null, null, null, new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }

            //2.遍历所有表
            for (String tableName : tables) {

                StringBuilder sql = new StringBuilder();
                sql.append("select * from " + tableName);
                ps = con.prepareStatement(String.valueOf(sql));
                rs = ps.executeQuery();
                meta = rs.getMetaData();

                System.out.println("表 " + tableName + "共有 " + meta.getColumnCount() + "个字段.");
                //3.获取字段
                for (int i = 1; i < meta.getColumnCount() + 1; i++) {
                    columns.add(meta.getColumnName(i));
                    System.out.println("字段名：" + meta.getColumnName(i));
                }
                columns.clear();
            }
            tables.clear();

        }


    }

    /**
     * 测试类
     *
     * @param args
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        Map<String, String> map = new IdentityHashMap<>();
        map.put(new String("jdbc:mysql://localhost:3306/jdyp_topic"), new String("root=123456"));
        map.put(new String("jdbc:mysql://localhost:3306/jdyp_depot"), new String("root=123456"));

        dataBaseInfo(map);

    }
}
