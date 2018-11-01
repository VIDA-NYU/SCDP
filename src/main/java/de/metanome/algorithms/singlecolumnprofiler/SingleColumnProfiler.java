package de.metanome.algorithms.singlecolumnprofiler;

import java.io.IOException;

public class SingleColumnProfiler {

    public static void main(String[] args) throws IOException {
        
        String filePath = args[0];
        
        SingleColumnProfilerAlgorithm algorithm = new SingleColumnProfilerAlgorithm();
        algorithm.execute("", filePath);

    }

}
