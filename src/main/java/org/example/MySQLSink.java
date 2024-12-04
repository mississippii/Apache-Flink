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
import java.util.concurrent.TimeUnit;

public class MySQLSink extends RichSinkFunction<Tuple2<String, Integer>> {

    private static final String MYSQL_URL = "jdbc:mysql://103.95.96.98:3306/ccl1";
    private static final String USER = "tbuser";
    private static final String PASSWORD = "Takay1takaane$";
    private static final String SQL = "INSERT INTO flinkWord (word, count, entry_time) VALUES (?, ?, ?)";
    private transient Connection connection;
    private transient PreparedStatement preparedStatement;
    private static final Logger logger = LoggerFactory.getLogger(MySQLSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connection = DriverManager.getConnection(MYSQL_URL, USER, PASSWORD);
            preparedStatement = connection.prepareStatement(SQL);
        } catch (SQLException e) {
            logger.error("Failed to establish MySQL connection", e);
            throw new RuntimeException("Failed to establish MySQL connection", e);
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        int retries = 3;
        while (retries > 0) {
            try {
                preparedStatement.setString(1, value.f0); // word
                preparedStatement.setInt(2, value.f1);    // count
                preparedStatement.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
                preparedStatement.executeUpdate();
                logger.info("Inserted data: word = {}, count = {}, entry_time = {}", value.f0, value.f1, System.currentTimeMillis());
                return;
            } catch (SQLException e) {
                if (isLockException(e)) {
                    retries--;
                    if (retries == 0) {
                        throw new RuntimeException("MySQL insertion failed due to lock issues", e);
                    }
                    TimeUnit.SECONDS.sleep(1); // Backoff before retry
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
