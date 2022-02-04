import java.io.IOException;
import java.lang.Math;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private float distance(float x1, float x2, float y1, float y2){
            return (float) Math.sqrt((y2 - y1) * (y2 - y1) + (x2 - x1) * (x2 - x1));
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] record = value.toString().split(",");
            float x_value = Float.valueOf(record[0]);
            float y_value = Float.valueOf(record[1]);
            Point pt = new Point(x_value, y_value);

            String[] cens = context.getConfiguration().get("centroids").toString().split("\t");
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
            context.write(new Text(closest_centroid.toString()), new Text(pt.toString()));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public void debug(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("centroids", args[0]); //TODO replace args[0] "(8,1) (1,2432)"
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}