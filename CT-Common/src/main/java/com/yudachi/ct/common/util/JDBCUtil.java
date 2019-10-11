package com.yudachi.ct.common.util;

import java.sql.Connection;
import java.sql.DriverManager;

public class JDBCUtil {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/ct?userUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWROD = "ZUIEWANGGUAN";

    public static Connection getConnection(){
        Connection connection = null;
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWROD);
        }catch (Exception e){
            e.printStackTrace();
        }

        return connection;
    }
}
