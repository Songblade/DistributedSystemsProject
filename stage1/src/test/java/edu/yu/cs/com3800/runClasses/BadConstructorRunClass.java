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
