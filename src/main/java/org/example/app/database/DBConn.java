package org.example.app.database;

import org.example.app.view.AppView;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBConn {
    public static Connection connect() {

        Properties props = new Properties();
        Connection conn = null;

        try {
            props.load(DBConn.class.getResourceAsStream("/db/jdbc.properties"));
            conn = DriverManager.getConnection(
                    props.getProperty("dbDriver") + props.getProperty("dbName"),
                    props.getProperty("username"), props.getProperty("password"));
        } catch (SQLException | IOException e) {
            new AppView().getOutput(e.getMessage());
        }
        return conn;
    }
}
