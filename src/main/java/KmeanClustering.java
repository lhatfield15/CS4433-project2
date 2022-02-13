import java.io.*;
import java.lang.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeanClustering {

    static class Point implements Comparable<Point> {

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

        public String toString() {
            return Float.toString(this.x) + "," + Float.toString(this.y);
        }
    }

    public static class ClosestCentroidMapper
            extends Mapper<Object, Text, Text, Text> {

        private float distance(float x1, float x2, float y1, float y2) {
            return (float) Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // get input point
            String[] record = value.toString().split(",");
            float x_value = Float.valueOf(record[0]);
            float y_value = Float.valueOf(record[1]);
            Point pt = new Point(x_value, y_value);

            // get centroids
            String[] cens = context.getConfiguration().get("centroids").toString().split(" "); // "" "1,2" "2,3"

            // find closest centroid to input point
            Point closest_centroid = null;
            float closest_distance = Float.MAX_VALUE;
            for (String cen : cens) {
                float cent_x = Float.parseFloat(cen.split(",")[0]);
                float cent_y = Float.parseFloat(cen.split(",")[1]);
                float dist = distance(pt.x, cent_x, pt.y, cent_y);
                if (dist < closest_distance) {
                    closest_centroid = new Point(cent_x, cent_y);
                    closest_distance = dist;
                }
            }

            // output closest centroid to pt
            context.write(new Text(closest_centroid.toString()), new Text(pt.toString()));
        }
    }

    public static class CentroidRecalculatorCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            // calculate center of all points in values to get new centroid

            // sum up totals
            float x_total = 0;
            float y_total = 0;
            float count = 0;
            for (Text pt : values) {
                String[] coords = pt.toString().split(",");
                float x = Float.valueOf(coords[0]);
                float y = Float.valueOf(coords[1]);
                x_total += x;
                y_total += y;
                count++;
            }

            // write new centroid pt to output file xtotal,ytotal,count
            context.write(key,
                    new Text(String.valueOf(x_total) + "," + String.valueOf(y_total) + "," + String.valueOf(count)));
        }
    }

    public static class CentroidRecalculatorReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            // calculate center of all points in values to get new centroid

            ArrayList<String> points = new ArrayList<>();
            // sum up totals
            float x_total = 0;
            float y_total = 0;
            float count = 0;
            for (Text pt : values) {
                String[] data = pt.toString().split(",");
                points.add(data[0] + "," + data[1]);
                float x = Float.valueOf(data[0]);
                float y = Float.valueOf(data[1]);
                float c = Float.valueOf(data[2]);
                x_total += x;
                y_total += y;
                count += c;
            }

            float new_centroid_x = x_total / count;
            float new_centroid_y = y_total / count;

            String out_style = context.getConfiguration().get("outputStyle").toString();

            // write new centroid pt to output file
            context.write(new Text("Centroid"),
                    new Text(String.valueOf(new_centroid_x) + "," + String.valueOf(new_centroid_y)));
            if (out_style.equals("b")) {
                for (String pt : points) {
                    context.write(new Text("Point"), new Text(pt));
                }
            }
        }
    }

    public static void runKMeansCycle(String[] input) throws Exception {
        // so we don't get the error if someone forgot to delete
        deleteOldOutputDirectory();
        // run cycle

        // initiliaze job
        Configuration conf = new Configuration();
        conf.set("centroids", input[2]);
        conf.set("outputStyle", input[4]);
        Job job = Job.getInstance(conf, "K means");
        job.setJarByClass(KmeanClustering.class);
        job.setCombinerClass(CentroidRecalculatorCombiner.class);
        job.setMapperClass(ClosestCentroidMapper.class);
        job.setReducerClass(CentroidRecalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input[0]));
        FileOutputFormat.setOutputPath(job, new Path(input[1]));

        // run job
        job.waitForCompletion(true);
    }

    public static ArrayList<Point> generateCentroidsFromFile() {
        // make a list of the new centroids
        ArrayList<Point> centroids = new ArrayList<Point>();
        // read from output path -- https://www.w3schools.com/java/java_files_read.asp

        try {
            // read the new centroids
            File myObj = new File(FileSystemBase.fileBase + outputFolder + outputFile);
            Scanner myReader = new Scanner(myObj);
            // read each line of points
            while (myReader.hasNextLine()) {
                // create point from coords and add to centroid list
                String data[] = myReader.nextLine().split("\t");
                if (data[0].equals("Centroid")) {
                    String coords[] = data[1].split(",");
                    centroids.add(new Point(Float.valueOf(coords[0]), Float.valueOf(coords[1])));
                }
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

    public static ArrayList<Point> generateCentroidsFromInput(String centroids_string) {
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

    public static boolean hasConverged(ArrayList<Point> new_centroids, ArrayList<Point> old_centroids,
            float convergence_distance_threshold) {
        // ensure the number of centroids has stayed the same since last iteration
        if (old_centroids.size() != new_centroids.size()) {
            if (new_centroids.size() > old_centroids.size()) {
                System.out.println("Problem, new centroids created");
                System.exit(1);
            } else {
                // generate a new point for a centroid
                Random rand = new Random();
                while (old_centroids.size() > new_centroids.size()) {
                    float x = rand.nextFloat() * 10000;
                    float y = rand.nextFloat() * 10000;
                    new_centroids.add(new Point(x, y));
                }
            }
        }

        // compare if the old centroids were within tolerance of new centroids
        for (int j = 0; j < old_centroids.size(); j++) {
            if (old_centroids.get(j).distance(new_centroids.get(j)) > convergence_distance_threshold) {
                return false;
            }
        }
        return true;
    }

    public static void deleteOldOutputDirectory() {
        // delete the old output directory
        try {
            FileUtils.deleteDirectory(new File(FileSystemBase.fileBase + outputFolder));
        } catch (IOException e) {
            System.out.println("deletion is unsuccessful");
        } catch (IllegalArgumentException e) {
            System.out.println("directory does not exist or is not a directory");
        }
    }

    public static String newInputFromCentroids(ArrayList<Point> centroids) {
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

    public static void appendConvergence(boolean hasConverged) {
        // Open given file in append mode by creating an
        // object of BufferedWriter class
        try {
            BufferedWriter out = new BufferedWriter(
                    new FileWriter(FileSystemBase.fileBase + outputFolder + outputFile, true));

            // Writing on output stream
            if (hasConverged) {
                out.write("The System Converged.");
            } else {
                out.write("The System Did Not Converge.");
            }
            // Closing the connection
            out.close();
        } catch (Exception e) {
            System.out.println("Error writing convergence to file");
        }
    }

    public static String readCentroidSeeds() {
        String centroids = "";// form of x,y x1,y1 x2,y2...
        try {
            // read the new centroids
            File myObj = new File(FileSystemBase.fileBase + "/seed_points.txt");
            Scanner myReader = new Scanner(myObj);
            // read each line of points
            while (myReader.hasNextLine()) {
                // create point from coords and add to centroid list
                centroids += " " + myReader.nextLine();
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("Couldn't find seed file.");
        }
        return centroids.substring(1);
    }

    public static String outputFile = "/part-r-00000";
    public static String outputFolder = "/KMeansOutput";

    public void debug(String[] args) throws Exception {
        args[2] = readCentroidSeeds();

        float convergence_distance_threshold = 1;
        boolean hasConverged = false;

        int R = Integer.valueOf(args[3]);
        for (int i = 0; i < R; i++) {
            // check for convergence only if not first cycle
            if (i > 0) {
                // acquire past (old) and present (new) centroids
                ArrayList<Point> new_centroids = generateCentroidsFromFile();
                ArrayList<Point> old_centroids = generateCentroidsFromInput(args[2]);

                // end program if centroids are in convergence
                if (hasConverged(new_centroids, old_centroids, convergence_distance_threshold)) {
                    hasConverged = true;
                    break;
                }

                args[2] = newInputFromCentroids(new_centroids);
            }

            // Run one iteration of kmeans clustering
             runKMeansCycle(args);
        } // end cycle for loop
        if (args[4].equals("a")) {
            appendConvergence(hasConverged);
        }
    }

    public static void main(String[] args) throws Exception {
        args[2] = readCentroidSeeds();

        float convergence_distance_threshold = 1;
        boolean hasConverged = false;

        int R = Integer.valueOf(args[3]);
        for (int i = 0; i < R; i++) {
            // check for convergence only if not first cycle
            if (i > 0) {
                // acquire past (old) and present (new) centroids
                ArrayList<Point> new_centroids = generateCentroidsFromFile();
                ArrayList<Point> old_centroids = generateCentroidsFromInput(args[2]);

                // end program if centroids are in convergence
                if (hasConverged(new_centroids, old_centroids, convergence_distance_threshold)) {
                    hasConverged = true;
                    break;
                }

                args[2] = newInputFromCentroids(new_centroids);
            }

            // Run one iteration of kmeans clustering
            // runKMeansCycle(args);
            deleteOldOutputDirectory();
            // run cycle

            // initiliaze job
            Configuration conf = new Configuration();
            conf.set("centroids", args[2]);
            conf.set("outputStyle", args[4]);
            Job job = Job.getInstance(conf, "K means");
            job.setJarByClass(KmeanClustering.class);
            job.setCombinerClass(CentroidRecalculatorCombiner.class);
            job.setMapperClass(ClosestCentroidMapper.class);
            job.setReducerClass(CentroidRecalculatorReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // run job
            job.waitForCompletion(true);
        } // end cycle for loop
        if (args[4].equals("a")) {
            appendConvergence(hasConverged);
        }
    }
}