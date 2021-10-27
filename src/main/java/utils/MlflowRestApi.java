package utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author xie yang 2021/10/27
 */
public class MlflowRestApi {
    public int mlPredict(float[] df) throws Exception{
        String url = "http://localhost:1234/invocations";
        URL serverUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) serverUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "application/json; utf-8");
        connection.setRequestProperty("Accept", "application/json");
        connection.setDoOutput(true);
        String jsonInputString = String.format("{ \"columns\":[\"x_avg\",\"y_avg\",\"z_avg\",\"vb_avg\",\"x_stddev\",\"y_stddev\",\"z_stddev\",\"vb_stddev\",\"x_var\",\"y_var\",\"z_var\",\"vb_var\",\"x_rms\",\"y_rms\",\"z_rms\",\"vb_rms\"], \"data\": [ [%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s]]}",
                df[0],df[1],df[2],df[3],df[4],df[5],df[6],df[7],df[8],df[9],df[10],df[11],df[12],df[13],df[14],df[15]);
        try(OutputStream os = connection.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
        if (connection.getResponseCode() != 200) {
            throw new RuntimeException(
                    "HTTP GET Request Failed with Error code : "
                            + connection.getResponseCode());
        }
        BufferedReader responseBuffer = new BufferedReader(
                new InputStreamReader((connection.getInputStream())));
        String output;
        //System.out.println("Output from Server:  \n");
        while ((output = responseBuffer.readLine()) != null) {
            return (int) Float.parseFloat(output.replace("[","").replace("]",""));
            //System.out.println(output);
        }
        connection.disconnect();
        return -1;
    }
}
