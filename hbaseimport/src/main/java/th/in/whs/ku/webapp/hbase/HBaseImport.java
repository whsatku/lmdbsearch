package th.in.whs.ku.webapp.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class HBaseImport {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("textinputformat.record.delimiter", "\r\n\r\n");
        conf.set("mapred.compress.map.output", "true");
        Job job = Job.getInstance(conf, "HBaseImport");
        job.setJarByClass(HBaseImport.class);

        job.setMapperClass(ParagraphMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        TextInputFormat.setInputPaths(job, new Path(args[0]));

        TableMapReduceUtil.initTableReducerJob("GTBTXT", HBaseReducer.class, job);

        job.waitForCompletion(true);
    }
}
