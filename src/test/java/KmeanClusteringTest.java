import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class KmeanClusteringTest {

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
            float x2y2 = (x_difference * x_difference) + (y_difference * y_difference);
            return (float) Math.sqrt(x2y2);
        }

        @Override
        public int compareTo(Point pt) {
            if (this.x > pt.x) {
                return 1; // this is bigger
            } else if (this.x == pt.x && this.y > pt.y) {
                return 1; // this is bigger
            } else if (this.x == pt.x && this.y == pt.y) {
                return 0; // this and object are same
            } else {
                return -1; // this is smaller
            }
        }
    }

    public void runKMeansCycle(String[] input) throws Exception {
        KmeanClustering wc = new KmeanClustering();
        wc.debug(input);
    }

    public ArrayList<Point> generateCentroidsFromFile() {
        // make a list of the new centroids
        ArrayList<Point> centroids = new ArrayList<Point>();
        // read from output path -- https://www.w3schools.com/java/java_files_read.asp

        try {
            // read the new centroids
            File myObj = new File(FileSystemBase.fileBase + outputFolder + "/part-r-00000");
            Scanner myReader = new Scanner(myObj);
            // read each line of points
            while (myReader.hasNextLine()) {
                // create point from coords and add to centroid list
                String data[] = myReader.nextLine().split("\t");
                centroids.add(new Point(Float.valueOf(data[0]), Float.valueOf(data[1])));
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error reading file");
        }

        // order the points in the centroids, so that each centroid is compared
        // to the same one as last time
        Collections.sort(centroids);
        return centroids;
    }

    public ArrayList<Point> generateCentroidsFromInput(String centroids_string) {
        // get a list of the previously input centroids
        ArrayList<Point> centroids = new ArrayList<>();
        String[] points = centroids_string.split(" ");// get centroids
        // put old centroids into list as Points
        for (String pt : points) {
            String[] coords = pt.split(",");
            centroids.add(new Point(Float.valueOf(coords[0]), Float.valueOf(coords[1])));
        }

        return centroids;
    }

    public boolean hasConverged(ArrayList<Point> new_centroids, ArrayList<Point> old_centroids,
            float convergence_distance_threshold) {
        // ensure the number of centroids has stayed the same since last iteration
        if (old_centroids.size() != new_centroids.size()) {
            System.out.println("New and Old Centroid count differed");
        }

        // compare if the old centroids were within tolerance of new centroids
        for (int j = 0; j < old_centroids.size(); j++) {
            if (old_centroids.get(j).distance(new_centroids.get(j)) > convergence_distance_threshold) {
                return false;
            }
        }
        return true;
    }

    public void deleteOldOutputDirectory() {
        // delete the old output directory
        try {
            FileUtils.deleteDirectory(new File(FileSystemBase.fileBase + outputFolder));
        } catch (IOException e) {
            System.out.println("deletion is unsuccessful");
        } catch (IllegalArgumentException e) {
            System.out.println("directory does not exist or is not a directory");
        }
    }

    public String newInputFromCentroids(ArrayList<Point> centroids) {
        // convert the new centroids into a form for passing to mapreduce
        String new_input = "";
        if (centroids.size() == 0) {
            System.out.println("There were no centroids. This is an error");
        }
        for (Point p : centroids) {
            new_input += " " + String.valueOf(p.x) + "," + String.valueOf(p.y);
        }
        return new_input.substring(1);
    }

    public String outputFolder = "/KMeansOutput";

    @Test
    public void debug() throws Exception {
        String[] input = new String[5];

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
        input[1] = "file:///" + FileSystemBase.fileBase + outputFolder; // output
        input[2] = "250,250 5250,5260"; // initial centroids
        input[3] = "5"; // number of iterations
        input[4] = "a"; // output centroids/convergence = a, output clustered data points = 'b'

        boolean reset = false;
        float convergence_distance_threshold = 1;

        if (reset) {
            // run to reset
            runKMeansCycle(input);
        } else {
            int R = Integer.valueOf(input[3]);
            for (int i = 0; i < R; i++) {
                // check for convergence only if not first cycle
                if (i > 0) {
                    // acquire past (old) and present (new) centroids
                    ArrayList<Point> new_centroids = generateCentroidsFromFile();
                    ArrayList<Point> old_centroids = generateCentroidsFromInput(input[2]);

                    // end program if centroids are in convergence
                    if (hasConverged(new_centroids, old_centroids, convergence_distance_threshold)) {
                        System.out.println("Points are in convergence tolerance");
                        break;
                    }

                    input[2] = newInputFromCentroids(new_centroids);
                }

                // so we don't get the error
                deleteOldOutputDirectory();
                // Run one iteration of kmeans clustering
                runKMeansCycle(input);
            }
        } // end reset else
    }
}