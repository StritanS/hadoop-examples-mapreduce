package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtsContainingTrees", DistrictsContainingTrees.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("speciesList", SpeciesList.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("treeKindCount", TreeKindCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("treeKindHeight", TreeKindHeight.class,
                    "A map/reduce program that counts the words in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
