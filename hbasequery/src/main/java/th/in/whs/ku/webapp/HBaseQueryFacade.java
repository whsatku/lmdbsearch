package th.in.whs.ku.webapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Bytes;
import th.in.whs.ku.webapp.bloom.BloomQuery;

import java.io.IOException;

public class HBaseQueryFacade {
    public static final String TABLE_NAME = "GTBTXT";
    public static final byte[] COL_FAMILY = Bytes.toBytes("p");
    public static final byte[] COL_FILE = Bytes.toBytes("f");
    public static final byte[] COL_PARAGRAPH = Bytes.toBytes("p");
    public static final String ZOOKEEPER_QUORUM = "master.webapp.ku.whs.in.th,hd1.webapp.ku.whs.in.th,hd2.webapp.ku.whs.in.th";
    public static final int ZOOKEEPER_PORT = 2181;

    private Table table;
    private BloomQuery bloom = new BloomQuery("bloom.bin");

    public HBaseQueryFacade() throws IOException, ServiceException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        config.setInt("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);

        HBaseAdmin.checkHBaseAvailable(config);

        Connection con = ConnectionFactory.createConnection(config);
        table = con.getTable(TableName.valueOf(TABLE_NAME));
    }

    private FacadeResult hbaseQuery(String query){
        try {
            Result result = table.get(new Get(Bytes.toBytes(query)));
            if(result.isEmpty()){
                return null;
            }
            String filename = Bytes.toString(result.getValue(COL_FAMILY, COL_FILE));
            long paragraph = Bytes.toLong(result.getValue(COL_FAMILY, COL_PARAGRAPH));

            return new FacadeResult(filename, paragraph);
        } catch (IOException e) {
            return null;
        }
    }

    public FacadeResult query(String query){
        if(!bloom.query(query)){
            return null;
        }

        return hbaseQuery(query);
    }

    public static class FacadeResult{
        public String filename;
        public long paragraph;

        public FacadeResult(String filename, long paragraph) {
            this.filename = filename;
            this.paragraph = paragraph;
        }
    }
}
