import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeanReturnPt_Cen {

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
            KmeanClustering.Point pt = new KmeanClustering.Point(x_value, y_value);

            //get centroids
            String[] cens = context.getConfiguration().get("centroids").toString().split(" "); // "" "1,2" "2,3"

            //find closest centroid to input point
            KmeanClustering.Point closest_centroid = null;
            float closest_distance = Float.MAX_VALUE;
            for (String cen: cens){
                float cent_x = Float.parseFloat(cen.split(",")[0]);
                float cent_y = Float.parseFloat(cen.split(",")[1]);
                float dist = distance(pt.x, cent_x, pt.y, cent_y);
                if (dist < closest_distance) {
                    closest_centroid = new KmeanClustering.Point(cent_x, cent_y);
                    closest_distance = dist;
                }
            }

            //output closest centroid to pt
            context.write(new Text(closest_centroid.toString()), new Text(pt.toString()));
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("centroids", args[2]);
        Job job = Job.getInstance(conf, "K means ptcen");
        job.setJarByClass(KmeanReturnPt_Cen.class);

        job.setMapperClass(KmeanReturnPt_Cen.ClosestCentroidMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        //run job and finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("centroids", args[2]);
        Job job = Job.getInstance(conf, "K means ptcen");
        job.setJarByClass(KmeanReturnPt_Cen.class);

        job.setMapperClass(KmeanReturnPt_Cen.ClosestCentroidMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        //run job and finish
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}