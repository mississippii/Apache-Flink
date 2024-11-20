package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLSink extends RichSinkFunction<Tuple2<String, Integer>> {

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/microservice", "root", "1234");
            String sql = "INSERT INTO flinkWord (word, count) VALUES (?, ?)";
            preparedStatement = connection.prepareStatement(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to establish MySQL connection", e);
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        boolean success = false;
        int retries = 0;
        while (retries < 3 && !success) {
            try {
                preparedStatement.setString(1, value.f0);
                preparedStatement.setInt(2, value.f1);
                preparedStatement.executeUpdate();
                success = true;
            } catch (SQLException e) {
                if (isLockException(e)) {
                    retries++;

                    if (retries >= 3) {
                        throw new RuntimeException("MySQL insertion failed due to lock issues", e);
                    }
                    Thread.sleep(1000);
                } else {

                    throw new RuntimeException("Failed to insert data into MySQL", e);
                }
            }
        }
    }

    private boolean isLockException(SQLException e) {
        String sqlState = e.getSQLState();
        return "40001".equals(sqlState) || "41000".equals(sqlState);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close MySQL statement", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to close MySQL connection", e);
            }
        }
    }
}
