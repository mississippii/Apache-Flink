package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;


public class RestSink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) throws Exception {
        String targetUrl = "https://kothasms.bdcom.com/api/v2/trans/sendSms";
        URL url = new URL(targetUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        String transactionId = generateUniqueTransactionId();

        String jsonInput = "{"
                + "\"username\": \"telcobright_test\","
                + "\"password\": \"#Sharedp123\","
                + "\"msisdn\": \"01932590638\","
                + "\"cli\": \"8809666666668\","
                + "\"message\": \"" + value + "\","
                + "\"clienttransid\": \""+transactionId+"\","
                + "\"rn_code\": \"71\","
                + "\"type\": \"SMS2\","
                + "\"longSMS\": \"\","
                + "\"isLongSMS\": \"false\","
                + "\"dataCoding\": \"gsm7\","
                + "\"isUnicode\": \"false\","
                + "\"unicode\": \"unicode\","
                + "\"isFlash\": \"false\","
                + "\"flash\": \"noflash\""
                + "}";

        try (OutputStream os = connection.getOutputStream()) {
            os.write(jsonInput.getBytes());
            os.flush();
        }

        int responseCode = connection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            System.err.println("Failed to send message: HTTP code " + responseCode);
        } else {
            System.out.println(value +"Message sent successfully!");
        }
        connection.disconnect();
    }
    public  String generateUniqueTransactionId() {
        String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String digits = "0123456789";
        StringBuilder transactionId = new StringBuilder();

        transactionId.append("TXN");
        transactionId.append(generateRandomString(digits, 3)); // Three digits
        transactionId.append(generateRandomString(characters, 1)); // One letter
        transactionId.append(generateRandomString(digits, 2)); // Two digits
        transactionId.append(generateRandomString(characters, 1)); // One letter
        transactionId.append(generateRandomString(digits, 2)); // Two digits
        transactionId.append(generateRandomString(characters, 1)); // One letter
        transactionId.append(generateRandomString(digits, 1)); // One d

        return transactionId.toString();
    }
    private  String generateRandomString(String characterSet, int length) {
        Random random = new Random();
        StringBuilder randomString = new StringBuilder();

        for (int i = 0; i < length; i++) {
            randomString.append(characterSet.charAt(random.nextInt(characterSet.length())));
        }

        return randomString.toString();
    }
}
