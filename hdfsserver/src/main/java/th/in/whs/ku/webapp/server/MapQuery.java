package th.in.whs.ku.webapp.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import th.in.whs.ku.webapp.bloom.BloomQuery;

import java.io.IOException;

public class MapQuery {

    private MapFile.Reader reader = null;
    static IntWritable FILENAME = new IntWritable(1);
    static IntWritable PARAGRAPH = new IntWritable(2);

    private BloomQuery bloom = new BloomQuery("bloom.bin");

    public MapQuery() throws IOException {
        Configuration conf = new Configuration();
        reader = new MapFile.Reader(new Path("hdfs://master.webapp.ku.whs.in.th:8020/user/ubuntu/mapped/part-r-00000"), conf);
    }

    private Result hdfsQuery(String query) throws IOException {
        Text q = new Text(query);
        MapWritable out = new MapWritable();
        reader.get(q, out); // writes to the arguments out
        if(out.isEmpty()){
            return null;
        }
        return new Result(
                ((Text) out.get(FILENAME)).toString(),
                ((LongWritable) out.get(PARAGRAPH)).get()
        );
    }

    public Result query(String query){
        if(!bloom.query(query)){
            return null;
        }

        try {
            return hdfsQuery(query);
        } catch (IOException e) {
            return null;
        }
    }

    public static class Result{
        public String filename;
        public long paragraph;

        public Result(String filename, long paragraph) {
            this.filename = filename;
            this.paragraph = paragraph;
        }
    }


}
