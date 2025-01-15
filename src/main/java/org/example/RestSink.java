package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;


public class RestSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {

        HttpURLConnection connection = getHttpURLConnection();
        String jsonInput = "{\n" +
                "  \"senderId\": \"8809666666668\",\n" +
                "  \"phoneNumbers\": \"8801767876110\",\n" +
                "  \"message\": \""+value+"\",\n" +
                "  \"charEncoding\": \"gsm7\"\n" +
                "}";

        try (OutputStream os = connection.getOutputStream()) {
            os.write(jsonInput.getBytes());
            os.flush();
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            System.err.println("Failed to send message: HTTP code " + responseCode);
        } else {
            System.out.println("Sent message to 01789896378: "+value);
        }
        connection.disconnect();
    }

    private HttpURLConnection getHttpURLConnection() throws IOException {
        URL url = new URL("https://kothasms.bdcom.com/ofbiz-spring/api/SmsTask/sendSMS");
        String token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJsb2dpbklkIjoidGVsY29icmlnaHRfdGVzdCIsImV4cCI6MTczMzk5MDQxNywicGFydHlJZCI6IjEwMTMwIn0.KR7Hs_bgqVwqFPZg2jBGc2MwURaQ98yygeM4HOhJHeQ";
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Authorization","Bearer "+token);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "*/*");
        connection.setDoOutput(true);
        return connection;
    }

}
