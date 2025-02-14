package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class ClientImpl implements Client {

    private final URL serverURL;
    private HttpURLConnection connection;

    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        serverURL = new URL("http", hostName, hostPort, "/compileandrun");
    }

    @Override
    public void sendCompileAndRunRequest(String src) throws IOException {
        // last minute, we were told to throw an IAE if src is null, so...
        if (src == null) {
            throw new IllegalArgumentException("src is null");
        }
        // I learned how to use the HttpURLConnection here:
        // https://stackoverflow.com/questions/26940410/using-httpurlconnection-to-post-in-java

        connection = (HttpURLConnection) serverURL.openConnection();
        connection.setDoOutput(true); // tell them we are doing a post method with output
        connection.setRequestProperty("Content-Type", "text/x-java-source"); // set headers
        // now we actually send stuff
        OutputStream output = connection.getOutputStream();
        output.write(src.getBytes());
        output.close();
    }

    @Override
    public Response getResponse() throws IOException {
        // just in case they called things out of order
        if (connection == null) {
            throw new IllegalStateException("getResponse called before sendCompileAndRunRequest");
        }
        // now we check the response
        int responseCode = connection.getResponseCode();

        InputStream response = connection.getErrorStream();
        if (response == null) {
            response = connection.getInputStream();
        }
        String responseMessage = new String(response.readAllBytes());
        // and we close the connection, since we aren't maintaining it
        connection.disconnect();
        connection = null; // so we have to call send again before we can call this method again

        // finally, we return things the way he wanted us to
        return new Response(responseCode, responseMessage);
    }
}
