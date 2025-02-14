package edu.yu.cs.com3800.multiJVM;

import edu.yu.cs.com3800.runClasses.SuccessfulRunnerClass;
import edu.yu.cs.com3800.stage1.HTTPClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ClientTest {

    private static final String PATH_BASE = "src/test/java/edu/yu/cs/com3800/runClasses/";

    private static String getCodeFromFile(String fileName) throws IOException {
        File file = new File(PATH_BASE + fileName);
        return Files.readString(file.toPath());
    }

    @Test
    public void clientTest() throws IOException {
        HTTPClient client = new HTTPClient("localhost", 8888);
        String expected = new SuccessfulRunnerClass().run();
        String code = getCodeFromFile("SuccessfulRunnerClass.java");
        HTTPClient.Response response = client.sendCompileAndRunRequest(code);

        Assertions.assertEquals(expected, response.getBody());
        Assertions.assertEquals(200, response.getCode());
    }
}
