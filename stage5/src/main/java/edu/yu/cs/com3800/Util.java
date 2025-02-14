package edu.yu.cs.com3800;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Util {

    public static byte[] readAllBytesFromNetwork(InputStream in)  {
        try {
            int tries = 0;
            while (in.available() == 0 && tries < 10) {
                try {
                    tries++;
                    Thread.currentThread().sleep(500);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        catch(IOException e){}
        return readAllBytes(in);
    }

    public static byte[] readAllBytes(InputStream in) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int numberRead;
        byte[] data = new byte[40960];
        try {
            while (in.available() > 0 && (numberRead = in.read(data, 0, data.length)) != -1   ) {
                buffer.write(data, 0, numberRead);
            }
        }catch(IOException e){}
        return buffer.toByteArray();
    }

    public static Thread startAsDaemon(Runnable run, String name) {
        Thread thread = new Thread(run, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    public static String getStackTrace(Exception e){
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        PrintStream myErr = new PrintStream(bas,true);
        e.printStackTrace(myErr);
        myErr.flush();
        myErr.close();
        return bas.toString();
    }

    // everything below here is my own method
    /**
     * This utility function allows us to send multiple messages at once over the network
     * Because otherwise, it only interprets receiving one of them
     * @param messages that we are sending
     * @return a byte array containing all the messages
     */
    public static byte[] combineMultipleMessages(Collection<Message> messages) {
        // format:
        // we have an int saying the number of bytes in the message, and then the message
        // for each message
        // if we have no messages, we respond with the string: empty
        if (messages.isEmpty()) {
            return "empty".getBytes();
        }
        int[] lengthsOfMessages = messages.stream().mapToInt(message->message.getNetworkPayload().length).toArray();
        int totalMessageLength = Arrays.stream(lengthsOfMessages).sum();
        ByteBuffer buffer = ByteBuffer.allocate(totalMessageLength + Integer.BYTES * lengthsOfMessages.length);
        for (Message message : messages) {
            byte[] payload = message.getNetworkPayload();
            buffer.putInt(payload.length);
            buffer.put(payload);
        }

        return buffer.array();
    }

    /**
     * Undoes the previous method
     * @param networkPayload received containing multiple methods
     * @return a collection containing all the messages found here
     */
    public static Collection<Message> extractMultipleMessages(byte[] networkPayload) {
        if (Arrays.equals(networkPayload, "empty".getBytes())) {
            return List.of();
        }
        List<Message> messages = new LinkedList<>();
        ByteBuffer buffer = ByteBuffer.wrap(networkPayload);
        int currentLength;
        for (int i = 0; i < networkPayload.length; i += currentLength + 4) {
            currentLength = buffer.getInt();
            byte[] messagePayload = new byte[currentLength];
            buffer.get(messagePayload);
            messages.add(new Message(messagePayload));
        }

        return messages;
    }

    // I decided I wanted a HashMap with O(1) access of values
    // This should be in its own file, but we aren't allowed to add more
    // So instead, we have a monster file
    // Note: This class assumes that each value will only appear once, as a set
    // But it doesn't track to make sure of this
    // Note that the values may be a bit behind the keys in terms of updating given concurrent modifications
    // Note: This class is not fully implemented
    // Specifically, containsValue only updates for keys added when the map is created
    // and for keys removed with remove
    // Otherwise, it doesn't
    // Because I'm not using them in this assignment
    // And I don't want to have to implement an entire data structure
    // I think this only works because I don't have multiple threads modifying it at the same time
    // Otherwise, you could have race conditions if they modify the same key
    // But here, only the main thread drops things from the map
    public static class ConcurrentValuesMap<K, V> extends ConcurrentHashMap<K, V> {

        private final Map<V, K> valueMap;

        public ConcurrentValuesMap(Map<? extends K,? extends V> map) {
            super(map);
            valueMap = new ConcurrentHashMap<>();
            for (K key : map.keySet()) {
                valueMap.put(map.get(key), key);
            }
        }

        /**
         * Returns {@code true} if this map maps one or more keys to the
         * specified value. This version is O(1)!
         *
         * @param value value whose presence in this map is to be tested
         * @return {@code true} if this map maps one or more keys to the
         * specified value
         * @throws NullPointerException if the specified value is null
         */
        @Override
        public boolean containsValue(Object value) {
            return valueMap.containsKey(value);
            // I'm not sure why IntelliJ is suspicious of the call
        }

        /**
         * When given a value, gives the reverse mapping key
         * @param value you want the key for
         * @return the key corresponding to that value
         */
        public K getKey(V value) {
            return valueMap.get(value);
        }

        // It's not a problem that I didn't update values
        // Because it's only used for a for-loop, so the lack of O(1) random access doesn't matter

        /**
         * Removes the key (and its corresponding value) from this map.
         * This method does nothing if the key is not in the map.
         *
         * @param key the key that needs to be removed
         * @return the previous value associated with {@code key}, or
         * {@code null} if there was no mapping for {@code key}
         * @throws NullPointerException if the specified key is null
         */
        @Override
        public V remove(Object key) {
            V removedValue = super.remove(key);
            valueMap.remove(removedValue);
            return removedValue;
        }
    }

    public record Result(byte[] resultBinary, boolean errorOccurred, long jobID) {}

    // unfortunately, maven won't save test classes
    // so, I have to move my run classes here
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Need code to run");
            return;
        }
        String fileName = args[0];
        String code = "N/A";
        if (!args[0].equals("N/A")) {
            code = Files.readString(new File(fileName).toPath());
        }
        String endpoint = args.length == 1? "/compileandrun": args[1];
        String codeExtra = args.length < 3? "localhost": args[2];
        String hostname = args.length < 4? "localhost": args[3];
        HTTPClient client = new HTTPClient(hostname, 8888, endpoint);
        HTTPClient.Response response = client.sendCompileAndRunRequest(code + "\n//" + codeExtra);
        if ("/checkcluster".equals(endpoint)) {
            System.out.println(response.body());
        } else {
            System.out.println("Response " + response.code() + "\n" + response.body());
        }

    }

    // the main version is in my tests
    // this is just here so I can use it for the main method
    // so that I can access it from the jar file
    private static class HTTPClient {

        // this is the client that I'm using in my tests
        // It is a slightly modified version of the client from Stage1

        private final URL serverURL;
        private HttpURLConnection connection;

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

        public record Response(int code, String body, boolean cached) {}
    }


}
