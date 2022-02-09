import java.io.IOException;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeanClustering {

    public static class Point {
        float x;
        float y;

        public Point(float pt_x, float pt_y) {
            this.x = pt_x;
            this.y = pt_y;
        }

        public String toString(){
            return Float.toString(this.x)  + "," + Float.toString(this.y);
        }
    }

    public static class ClosestCentroidMapper
            extends Mapper<Object, Text, Text, Text>{

        private float distance(float x1, float x2, float y1, float y2){
            return (float) Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //get input point
            String[] record = value.toString().split(",");
            float x_value = Float.valueOf(record[0]);
            float y_value = Float.valueOf(record[1]);
            Point pt = new Point(x_value, y_value);

            //get centroids
            String[] cens = context.getConfiguration().get("centroids").toString().split(" "); // "" "1,2" "2,3"

            //find closest centroid to input point
            Point closest_centroid = null;
            float closest_distance = Float.MAX_VALUE;
            for (String cen: cens){
                float cent_x = Float.parseFloat(cen.split(",")[0]);
                float cent_y = Float.parseFloat(cen.split(",")[1]);
                float dist = distance(pt.x, cent_x, pt.y, cent_y);
                if (dist < closest_distance) {
                    closest_centroid = new Point(cent_x, cent_y);
                    closest_distance = dist;
                }
            }

            //output closest centroid to pt
            context.write(new Text(closest_centroid.toString()), new Text(pt.toString()));
        }
    }

    public static class CentroidRecalculatorReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //calculate center of all points in values to get new centroid

            //sum up totals
            float x_total = 0;
            float y_total = 0;
            float count = 0;
            for(Text pt: values){
                String[] coords = pt.toString().split(",");
                float x = Float.valueOf(coords[0]);
                float y = Float.valueOf(coords[1]);
                x_total += x;
                y_total += y;
                count++;
            }

            //find new centroid
            float new_centroid_x = x_total/count;
            float new_centroid_y = y_total/count;

            //write new centroid pt to output file
            context.write(new Text(String.valueOf(new_centroid_x)), new Text(String.valueOf(new_centroid_y)));
        }
    }

    public void debug(String[] args) throws Exception{
        //initiliaze job
        Configuration conf = new Configuration();
        conf.set("centroids", args[2]);
        Job job = Job.getInstance(conf, "K means");
        job.setJarByClass(KmeanClustering.class);
        job.setMapperClass(ClosestCentroidMapper.class);
        job.setReducerClass(CentroidRecalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //run job
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(ClosestCentroidMapper.class);
        job.setCombinerClass(CentroidRecalculatorReducer.class);
        job.setReducerClass(CentroidRecalculatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}