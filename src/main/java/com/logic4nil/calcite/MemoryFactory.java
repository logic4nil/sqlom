package com.logic4nil.calcite;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class MemoryFactory {
    public static final MemoryFactory INSANCE = new MemoryFactory();
    public static Connection CONN = null;

    public CalciteConnection newConnection() throws SQLException {

        Properties info = new Properties();
        info.setProperty("lex", "JAVA");

        // Create Connection
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        return calciteConnection;
    }

    /*
     * 获取Memory Connection, 可以使用默认的Connection
     */
    public static Connection getConnection(boolean usedefault) throws SQLException {
        synchronized (MemoryFactory.class){
            if(MemoryFactory.CONN == null){
                MemoryFactory.CONN = INSANCE.newConnection();
            }
        }

        if(usedefault){
            return MemoryFactory.CONN;
        } else {
            return INSANCE.newConnection();
        }
    }
}
