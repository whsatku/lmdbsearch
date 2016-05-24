package th.in.whs.ku.webapp.lmdb;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.fusesource.lmdbjni.*;
import th.in.whs.ku.webapp.common.Hasher;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Loader {

    private static final String DELIMITER = "\r\n\r\n";

    private final Database textDatabase;
    private final Database fileDatabase;
    private final Env textEnv;
    private final Env fileEnv;

    public Loader(String textPath, String filePath){
        textEnv = new Env(textPath);
        textEnv.setMapSize(1, ByteUnit.GIBIBYTES);
        fileEnv = new Env(filePath);
        textDatabase = textEnv.openDatabase("text", Constants.CREATE | Constants.INTEGERKEY);
        fileDatabase = fileEnv.openDatabase("file", Constants.CREATE | Constants.INTEGERKEY);
    }

    public void loadFile(int id, File file) throws FileNotFoundException {
        Scanner scan = new Scanner(file).useDelimiter(DELIMITER);
        byte[] fileId = Ints.toByteArray(id);
        Transaction tx = textEnv.createWriteTransaction();
        Transaction fileTx = fileEnv.createWriteTransaction();

        fileDatabase.put(fileTx, fileId, file.getName().getBytes());

        int paragraph = 1;
        while(scan.hasNext()){
            String item = scan.next().replace("\r\n", " ");
            String[] tokens = item.split("\\. ");
            for(String token: tokens){
                token = token.trim();
                if(token.isEmpty()){
                    continue;
                }
                byte[] hash = Hasher.hash(token);

                byte[] value = Bytes.concat(
                        fileId,
                        Ints.toByteArray(paragraph)
                );
                assert value.length == 8;

                textDatabase.put(tx, hash, value);
            }
            paragraph++;
        }
        tx.commit();
        fileTx.commit();
    }

    public static void main(String[] args){
        try {
            Loader loader = new Loader("db/text", "db/file");

            int count = 0;
            for(String file: args) {
                loader.loadFile(count, new File(file));
                count++;
                System.out.printf("%d/%d\r", count, args.length);
            }
            System.out.println();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
