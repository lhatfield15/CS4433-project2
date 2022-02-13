import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public class KmeanClusteringTest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[6];

        /*
         * 1. put the data.txt into a folder in your pc
         * 2. add the path for the following two files.
         * windows : update the path like
         * "file:///C:/Users/.../projectDirectory/data.txt"
         * mac or linux: update the path like
         * "file:///Users/.../projectDirectory/data.txt"
         */

        // This System property thing was breaking things
        // System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        input[0] = "file:///" + FileSystemBase.fileBase + "/data_points.txt"; // input
        input[1] = "file:///" + FileSystemBase.fileBase + KmeanClustering.outputFolder; // output
        input[2] = "";// will be filled by program later as centroid seed
        input[3] = "10"; // number of iterations
        input[4] = "a"; // output centroids/convergence = a, output clustered data points = 'b'
        input[5] = "1"; // convergence_distance_threshold

        KmeanClustering km = new KmeanClustering();
        km.debug(input);

    }// end debug
}