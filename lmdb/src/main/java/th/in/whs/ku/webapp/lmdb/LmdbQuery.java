package th.in.whs.ku.webapp.lmdb;

import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import org.fusesource.lmdbjni.*;
import th.in.whs.ku.webapp.common.Hasher;

import java.util.Arrays;

public class LmdbQuery {
    private final Database textDatabase;
    private final Database fileDatabase;
    private final Env textEnv;
    private final Env fileEnv;

    public LmdbQuery(String textPath, String filePath){
        textEnv = new Env(textPath);
        textEnv.setMapSize(1, ByteUnit.GIBIBYTES);
        fileEnv = new Env(filePath);
        textDatabase = textEnv.openDatabase("text", Constants.CREATE | Constants.INTEGERKEY);
        fileDatabase = fileEnv.openDatabase("file", Constants.CREATE | Constants.INTEGERKEY);
    }

    public Result check(String text){
        Transaction tx = textEnv.createReadTransaction();
        byte[] result = textDatabase.get(tx, Hasher.hash(text.trim()));
        tx.close();

        if(result == null){
            return null;
        }

        byte[] fileId = Arrays.copyOfRange(result, 0, 4);
        assert fileId.length == 4;
        int paragraphId = Ints.fromBytes(result[4], result[5], result[6], result[7]);

        Transaction fileTx = fileEnv.createReadTransaction();
        String fileName = new String(fileDatabase.get(fileTx, fileId));
        fileTx.close();

        return new Result(fileName, paragraphId);
    }

    public static class Result{
        public String file;
        public int paragraph;

        public Result(String file, int paragraph) {
            this.file = file;
            this.paragraph = paragraph;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "file='" + file + '\'' +
                    ", paragraph=" + paragraph +
                    '}';
        }
    }

    public static void main(String[] args){
        LmdbQuery checker = new LmdbQuery("db/text", "db/file");
        String query = Joiner.on(' ').join(args);

        Result res = null;
        for(int i = 0; i < 1_000_000; i++) {
            res = checker.check(query);
        }
        System.out.println(res);
    }
}
