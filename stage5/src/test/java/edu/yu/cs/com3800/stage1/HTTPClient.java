package edu.yu.cs.com3800.stage1;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class HTTPClient {

    // this is the client that I'm using in my tests
    // It is a slightly modified version of the client from Stage1

    private final URL serverURL;
    private HttpURLConnection connection;

    public HTTPClient(String hostName, int hostPort) throws MalformedURLException {
        serverURL = new URL("http", hostName, hostPort, "/compileandrun");
    }

    public HTTPClient(String hostName, int hostPort, String endpoint) throws MalformedURLException {
        serverURL = new URL("http", hostName, hostPort, endpoint);
    }

    public Response sendCompileAndRunRequest(String src) throws IOException {

        connection = (HttpURLConnection) serverURL.openConnection();
        connection.setDoOutput(true); // tell them we are doing a post method with output
        connection.setRequestProperty("Content-Type", "text/x-java-source"); // set headers
        // now we actually send stuff
        OutputStream output = connection.getOutputStream();
        output.write(src.getBytes());
        output.close();

        return getResponse();
    }

    private Response getResponse() throws IOException {

        // now we check the response
        int responseCode = connection.getResponseCode();
        String wasCached = connection.getHeaderField("Cached-Response");

        InputStream response = connection.getErrorStream();
        if (response == null) {
            response = connection.getInputStream();
        }
        String responseMessage = new String(response.readAllBytes());
        // and we close the connection, since we aren't maintaining it
        connection.disconnect();

        // finally, we return things the way he wanted us to
        return new Response(responseCode, responseMessage, Boolean.parseBoolean(wasCached));
    }

    public static class Response {
        private final int code;
        private final String body;
        private final boolean cached;

        public Response(int code, String body, boolean cached) {
            this.code = code;
            this.body = body;
            this.cached = cached;
        }

        public int getCode() {
            return this.code;
        }

        public String getBody() {
            return this.body;
        }

        public boolean getCached() {
            return this.cached;
        }
    }
}
