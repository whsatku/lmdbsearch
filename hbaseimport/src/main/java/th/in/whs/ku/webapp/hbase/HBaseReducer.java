package th.in.whs.ku.webapp.hbase;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class HBaseReducer extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {
    public static byte[] COL_FAMILY = Bytes.toBytes("p");
    public static byte[] COL_FILE = Bytes.toBytes("f");
    public static byte[] COL_PARAGRAPH = Bytes.toBytes("p");

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        // only use one value per key
        MapWritable value = values.iterator().next();
        Text filename = (Text) value.get(ParagraphMapper.FILENAME);
        LongWritable paragraph = (LongWritable) value.get(ParagraphMapper.PARAGRAPH);

        Put put = new Put(Bytes.toBytes(key.toString()));
        put.add(COL_FAMILY, COL_FILE, Bytes.toBytes(filename.toString()));
        put.add(COL_FAMILY, COL_PARAGRAPH, Longs.toByteArray(paragraph.get()));

        context.write(null, put);
    }
}
