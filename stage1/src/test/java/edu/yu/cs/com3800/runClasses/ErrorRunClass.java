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
