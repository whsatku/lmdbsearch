package th.in.whs.ku.webapp.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

class ParagraphMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

    static IntWritable FILENAME = new IntWritable(1);
    static IntWritable PARAGRAPH = new IntWritable(2);

    private Text filename;
    private MapWritable out = new MapWritable();
    private Text textWritable = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        filename = new Text(fileName);
        out.put(FILENAME, filename);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString().replace("\r\n", " ");
        String[] tokens = text.split("\\. ");
        for(String token: tokens){
            token = token.trim();
            if(token.isEmpty()){
                continue;
            }

            out.put(PARAGRAPH, key);

            textWritable.set(token);
            context.write(textWritable, out);
        }
    }
}
