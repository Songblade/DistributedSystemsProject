package edu.yu.cs.com3800.stage1;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.SimpleServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SimpleServerImpl implements SimpleServer {

    private final HttpServer server;
    private final Logger logger;

    public SimpleServerImpl(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        logger = Logger.getLogger("server.logger");
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yy-MM-dd'T'hh-mm-ss");
        String currentDate = dateFormatter.format(new Date());
        Files.createDirectories(Paths.get("logs"));
        FileHandler handler = new FileHandler(System.getProperty("user.dir") + "/logs/log-" + currentDate + ".txt");
        logger.addHandler(handler);
        SimpleFormatter formatter = new SimpleFormatter();
        handler.setFormatter(formatter);
    }

    /**
     * start the server
     */
    @Override
    public void start() {
        server.createContext("/compileandrun", new SimpleHandler(logger));
        server.setExecutor(null);
        server.start();
        logger.info("Server started");
    }

    /**
     * stop the server
     */
    @Override
    public void stop() {
        server.stop(1);
        logger.info("Server stopped");
    }

    public static void main(String[] args) {
        int port = 9000;
        if(args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

    private static class SimpleHandler implements HttpHandler {

        private final Logger logger;

        private SimpleHandler(Logger logger) {
            this.logger = logger;
        }

        /**
         * Handle the given request and generate an appropriate response.
         * See {@link HttpExchange} for a description of the steps
         * involved in handling an exchange.
         *
         * @param exchange the exchange containing the request from the
         *                 client and used to send the response
         * @throws NullPointerException if exchange is {@code null}
         * @throws IOException          if an I/O error occurs
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("POST")) {
                exchange.sendResponseHeaders(405, -1);
                //sendResponse(exchange, 405, "Only POST methods accepted");
                logger.info("Sent 405 message");
                return;
            }
            Headers headers = exchange.getRequestHeaders();
            String contentHeader = headers.getFirst("Content-Type");
            if (!contentHeader.equals("text/x-java-source")) {
                sendResponse(exchange, 400, "Wrong data format, text/x-java-source required");
                logger.info("Send 400 response for contentHeader " + contentHeader);
                return;
            }

            // if they have the right format, let's try to run this
            InputStream requestBody = exchange.getRequestBody();
            JavaRunner runner = new JavaRunner();
            String result = null;
            try {
                // send a success response with the stuff they got
                result = runner.compileAndRun(requestBody);
                if (result == null) {
                    result = "null"; // because we can't return actual null
                }
            } catch (Exception e) {
                // find the error and return it to them
                // unfortunately, it is a bit complicated getting what he wants out of it
                String message = e.getMessage();
                StringWriter stackWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackWriter));
                String stackTrace = stackWriter.toString();
                String response = message + "\n" + stackTrace;
                //
                logger.info("Send 400 response for bad code");
                sendResponse(exchange, 400, response);
            }
            assert result != null;
            // success, let's send the result
            sendResponse(exchange, 200, result);
            logger.fine("Send 200 message");
        }

        private void sendResponse(HttpExchange exchange, int code, String response) throws IOException {
            exchange.sendResponseHeaders(code, response.length());
            OutputStream body = exchange.getResponseBody();
            body.write(response.getBytes());
            body.close();
        }

    }

}
