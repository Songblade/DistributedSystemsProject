package edu.yu.cs.com3800.stage1;

public class RunClasses {
    // I really should have been doing this a while ago
    // But instead of storing them in files and then translating them into strings, let's just store them as strings
    // Great opportunity to use block strings!
    // I like learning new functions of the language
    public static final String BAD_CONSTRUCTOR = """
            package edu.yu.cs.com3800.runClasses;
            
            public class BadConstructorRunClass {
            
                private final int i;
                public BadConstructorRunClass(int i) {
                    this.i = i;
                }
            
                public String run() {
                    return i == 42? "Correct": "Incorrect";
                }
            }
            """;

    public static final String BAD_IMPORT = """
            package edu.yu.cs.com3800.runClasses;
            
            import java.math.RoundingMode;
            
            public class BadImportRunClass {
                public BadImportRunClass() {}
            
                public String run() {
                    RoundingMode.valueOf("4");
                    return "Success!";
                }
            }
            """;

    public static final String BAD_RUN_METHOD = """
            package edu.yu.cs.com3800.runClasses;
            
            public class BadRunTestClass {
                private final int i;
                public BadRunTestClass() {
                    i = 10;
                }
            
                public int run() {
                    return i * 4 + 2;
                }
            
            }
            """;

    public static final String THROWS_EXCEPTION = """
            package edu.yu.cs.com3800.runClasses;
            
            public class ErrorRunClass {
                private final int i;
            
                ErrorRunClass() {
                    i = 2 + 2;
                }
            
                public String run() {
                    if (i == 5) {
                        return "Well done, the Big Brother appreciates your patriotism";
                    } else {
                        throw new IllegalStateException("2 + 2 != 5");
                    }
                }
            }
            """;

    public static final String MISSING_RUN_METHOD = """
            package edu.yu.cs.com3800.runClasses;
            
            public class MissingRunTestClass {
            
                public MissingRunTestClass() {
                    int i = 10;
                    System.out.println("Life, the universe, and everything means " + (i * 4 + 2));
                }
          
            }
            """;

    public static final String NULL_RETURN = """
            package edu.yu.cs.com3800.runClasses;
            
            public class NullReturnRunClass {
                public NullReturnRunClass() {}
            
                public String run() {
                    return null;
                }
            }
            """;

    public static final String PRIVATE_RUN_METHOD = """
            package edu.yu.cs.com3800.runClasses;
            
            public class PrivateRunClass {
                private final int i;
                public PrivateRunClass() {
                    i = 42;
                }
            
                private String run() {
                    return "Life is " + i;
                }
            }
            """;

    public static final String SUCCESSFUL_RUN = """
            package edu.yu.cs.com3800.runClasses;
            
            public class SuccessfulRunnerClass {
                private final int i;
                public SuccessfulRunnerClass() {
                    i = 10;
                }
            
                public String run() {
                    return "Life, the universe, and everything means " + (i * 4 + 2);
                }
            }
            """;

    public static final String BIG_WAIT = """
            package edu.yu.cs.com3800.runClasses;
            
            public class BigWaitRunnerClass {
            
                public String run() {
                    try {
                        Thread.sleep(10_000);
                        return "*Yawn* That was a big nap.";
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            """;

    public static String getBigWait(int sleepTime) {
        return BIG_WAIT.replace("10_000", "" + sleepTime);
    }
}
