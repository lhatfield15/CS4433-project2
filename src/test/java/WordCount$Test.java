import org.junit.Test;

import static org.junit.Assert.*;

public class WordCount$Test {
    public String outputFolder = "/KMeansOutput";

    @Test
    public void debug() throws Exception {
        String[] input = new String[5];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        input[0] = "file:///" + FileSystemBase.fileBase + "/data_points_TEST.txt";
        input[1] = "file:///" + FileSystemBase.fileBase + outputFolder;
        input[2] = "4904,121 5627.009,657.0088";
        input[3] = "2";
        input[4] = "file:///" + FileSystemBase.fileBase + "/TestResults/PtWithCentroids.txt";

        KmeanReturnPt_Cen wc = new KmeanReturnPt_Cen();
        wc.debug(input);
    }
}
