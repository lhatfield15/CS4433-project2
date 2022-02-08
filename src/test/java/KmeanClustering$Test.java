import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import static org.junit.Assert.*;

public class KmeanClustering$Test {

    @Test
    public void debug() throws Exception {
        String[] input = new String[4];

        /*
        1. put the data.txt into a folder in your pc
        2. add the path for the following two files.
            windows : update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            mac or linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        input[0] = "file:///C:/Users/Alex/IdeaProjects/CS4433_Project_2/data_points.txt";
        input[1] = "file:///C:/Users/Alex/IdeaProjects/CS4433_Project_2/Test_results/output.txt";
        input[2] = "4904,121 5627.009,657.0088";
        input[3] = "5";


        int R = Integer.valueOf(input[3]);
        for (int i = 0; i < R; i++){
            KmeanClustering wc = new KmeanClustering();
            wc.debug(input);

            //read from output path -- https://www.w3schools.com/java/java_files_read.asp
            String centroids = "";
            try {
                File myObj = new File(input[1] + "/" + "part-r-00000");
                Scanner myReader = new Scanner(myObj);
                while (myReader.hasNextLine()) {
                    String data[] = myReader.nextLine().split("\t");
                    centroids += " " + data[0] + "," + data[1];
                }
                //assume centroids will never be 0 chars
                input[2] = centroids.substring(1);
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred reading the output file.");
            }

            //delete output path -- https://www.w3schools.com/java/java_files_delete.asp
            File myObj = new File(input[1]);
            if (!myObj.delete()) {
                System.out.println("Failed to delete the output file.");
            }
        }
    }
}