package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<Tuple2<String,Integer>>{
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/microservices", "root", "Takay$ane");
        String sql = "INSERT INTO flink_word (word, count) VALUES (?, ?)";
        preparedStatement = connection.prepareStatement(sql);
    }
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        preparedStatement.setString(1, value.f0);
        preparedStatement.setInt(2, value.f1);
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }
}
