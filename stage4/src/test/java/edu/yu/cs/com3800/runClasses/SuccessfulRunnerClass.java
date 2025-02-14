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
