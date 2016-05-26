package th.in.whs.ku.webapp.hbase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

class ParagraphMapper extends Mapper<Object, Text, Text, MapWritable> {
    private static final String DELIMITER = "\r\n\r\n";

    static IntWritable FILENAME = new IntWritable(1);
    static IntWritable PARAGRAPH = new IntWritable(2);

    private Text filename;
    private MapWritable out = new MapWritable();
    private Text textWritable = new Text();
    private LongWritable longWritable = new LongWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        filename = new Text(fileName);
        out.put(FILENAME, filename);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] paragraphs = value.toString().split(DELIMITER);

        for(int i = 0; i < paragraphs.length; i++){
            String text = paragraphs[i].replace("\r\n", " ");
            String[] tokens = text.split("\\. ");
            longWritable.set(i + 1);
            for(String token: tokens){
                token = token.trim();
                if(token.isEmpty()){
                    continue;
                }

                out.put(PARAGRAPH, longWritable);

                textWritable.set(token);
                context.write(textWritable, out);
            }
        }
    }
}
