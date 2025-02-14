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

