package com.Java.MySql2Hive2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySqlInfo2 {

    private final static String DRIVER = "com.mysql.jdbc.Driver";
    private final static String URL = "jdbc:mysql://localhost:3306/jdyp_topic";
    private final static String USERNAME = "root";
    private final static String PASSWORD = "123456";

    public static List<String> getTableInfo() {

        Connection con = null;
        List<String> tables = new ArrayList<String>();
        try {
            con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            Class.forName(DRIVER);
            DatabaseMetaData dbMetaData = con.getMetaData();
            //得到数据库下所有表
            ResultSet rs = dbMetaData.getTables(null, null, null, new String[]{"TABLE"});

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

    public static List<String> getColumnName() {

        List<String> tables = getTableInfo();
        Connection conn = null;
        List<String> columns = new ArrayList<>();

        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            PreparedStatement ps = null;
            ResultSet rs = null;
            ResultSetMetaData meta = null;
            // 遍历数据库表，获取各表的字段等信息
            StringBuffer sbCloumns = new StringBuffer();

            for (String tableName : tables) {

                StringBuilder sql = new StringBuilder();
                sql.append("select * from " + tableName);
                ps = conn.prepareStatement(String.valueOf(sql));
                rs = ps.executeQuery();
                meta = rs.getMetaData();
                System.out.println("表 " + tableName + "共有 " + meta.getColumnCount() + "个字段.");

                for (int i = 1; i < meta.getColumnCount() + 1; i++) {
                    columns.add(meta.getColumnName(i));

                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columns;
    }

    /**
     * 测试类
     *
     * @param args
     */

    public static void main(String[] args) {
        //1.获取表名
        List<String> tables = getTableInfo();
        for (String name : tables) {
            System.out.println(name);
        }
        //2.获取字段名类型
        List<String> columns = getColumnName();
        for (String column : columns) {
            System.out.println("字段名：" + column);
        }
    }
}


