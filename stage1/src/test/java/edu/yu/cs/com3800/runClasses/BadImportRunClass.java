package edu.yu.cs.com3800.runClasses;

import java.math.RoundingMode;

public class BadImportRunClass {
    public BadImportRunClass() {}

    public String run() {
        RoundingMode.valueOf("4");
        return "Success!";
    }
}