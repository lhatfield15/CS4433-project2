import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class KmeanClustering$Test {

    class Point implements Comparable<Point> {

        float x;
        float y;

        public Point(float pt_x, float pt_y) {
            this.x = pt_x;
            this.y = pt_y;
        }

        public float distance(Point pt) {
            float x_difference = this.x - pt.x;
            float y_difference = this.y - pt.y;
            float x2y2 = (x_difference*x_difference) + (y_difference*y_difference);
            return (float) Math.sqrt(x2y2);
        }

        @Override
        public int compareTo(Point pt) {
            if (this.x > pt.x){
                return 1; //this is bigger
            }
            else if(this.x == pt.x && this.y > pt.y){
                return 1; //this is bigger
            }
            else if (this.x == pt.x && this.y == pt.y){
                return 0; // this and object are same
            }
            else{
                return -1; //this is smaller
            }
        }
    }

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

        //This System property thing was breaking things
        System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        input[0] = "file:///" + FileSystemBase.fileBase + "/data_points_TEST.txt";
        input[1] = "file:///" + FileSystemBase.fileBase + outputFolder;
        input[2] = "4904,121 5627.009,657.0088";
        input[3] = "2";
        input[4] = "file:///" + FileSystemBase.fileBase + "/TestResults/PtWithCentroids.txt";

        boolean reset = false;
        float convergence_distance_threshold = 1;

        if (reset) {
            //run to reset
            KmeanClustering wc = new KmeanClustering();
            wc.debug(input);
        } else {
            int R = Integer.valueOf(input[3]);
            for (int i = 0; i < R; i++) {
                //read from output path -- https://www.w3schools.com/java/java_files_read.asp
                try {
                    //read the new centroids
                    File myObj = new File(FileSystemBase.fileBase + outputFolder + "/part-r-00000");
                    Scanner myReader = new Scanner(myObj);

                    //make a list of the new centroids
                    ArrayList<Point> new_centroids = new ArrayList<Point>();
                    while (myReader.hasNextLine()) {
                        String data[] = myReader.nextLine().split("\t");
                        new_centroids.add(new Point(Float.valueOf(data[0]),Float.valueOf(data[1])));
                    }

                    // order the points in the centroids, so that each centroid is compared
                    // to the same one as last time
                    Collections.sort(new_centroids);

                    //get a list of the previously input centroids
                    ArrayList<Point> old_centroids = new ArrayList<>();
                    String[] points = input[2].split(" ");//get old centroids
                    // put old centroids into list as Points
                    for(String pt: points){
                        String[] coords = pt.split(",");
                        old_centroids.add(new Point(Float.valueOf(coords[0]), Float.valueOf(coords[1])));
                    }

                    //ensure the number of centroids has stayed the same since last iteration
                    if (old_centroids.size() != new_centroids.size()){
                        System.out.println("Something wrong happened");
                    }

                    //compare if the old centroids were within tolerance of new centroids
                    boolean outsideTolerance = false;
                    for(int j = 0; j < old_centroids.size(); j++){
                        if (old_centroids.get(j).distance(new_centroids.get(j)) > convergence_distance_threshold){
                            outsideTolerance = true;
                            break;
                        }
                    }
                    if (!outsideTolerance && i > 0){
                        //we are done with kmeans since no cluster are more than tolerance
                        //away from the previous cluster
                        System.out.println("Points are in convergence tolerance, breaked");
                        break;
                    }

                    //convert the new centroids into a form for passing to mapreduce
                    String newCentroids = "";
                    for (Point p: new_centroids){
                        newCentroids += " " + String.valueOf(p.x) + "," + String.valueOf(p.y);
                    }

                    //assume centroids will never be 0 chars
                    input[2] = newCentroids.substring(1);
                    myReader.close();
                } catch (FileNotFoundException e) {
                    System.out.println("Error finding file to read");
                }

                // delete the old output directory
                try {
                    FileUtils.deleteDirectory(new File(FileSystemBase.fileBase + outputFolder));
                }
                catch (IOException e){
                    System.out.println("deletion is unsuccessful");
                }
                catch (IllegalArgumentException e){
                    System.out.println("directory does not exist or is not a directory");
                }

                //Run one iteration of kmeans clustering
                KmeanClustering wc = new KmeanClustering();
                wc.debug(input);

                //the program did not break due to the convergence, output all points w/ centroids
                if (i == (R-1)) {
                    KmeanReturnPt_Cen.main(input);
                    System.out.println("Centroids did not converge in runs, output the current centroids w/ points");
                }
            }
        }//end reset else
    }
}