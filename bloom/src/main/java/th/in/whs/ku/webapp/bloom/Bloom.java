package th.in.whs.ku.webapp.bloom;

import com.google.common.primitives.UnsignedLongs;
import net.openhft.hashing.LongHashFunction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

public class Bloom {
    private static final String DELIMITER = "\r\n\r\n";

    private byte[] bloom;

    public Bloom(int size){
        bloom = new byte[size];
    }

    public void loadFile(File file) throws FileNotFoundException {
        Scanner scan = new Scanner(file).useDelimiter(DELIMITER);
        while(scan.hasNext()) {
            String item = scan.next().replace("\r\n", " ");
            String[] tokens = item.split("\\. ");
            for (String token : tokens) {
                token = token.trim();
                if (token.isEmpty()) {
                    continue;
                }
                long[] hashes = {
                        LongHashFunction.xx_r39().hashChars(token),
                        LongHashFunction.murmur_3().hashChars(token),
                };

                for(long hash: hashes){
                    int bucket = (int) (UnsignedLongs.divide(hash, 8) % bloom.length);
                    byte bit = (byte) UnsignedLongs.remainder(hash, 8);
                    bloom[bucket] |= 1 << bit;
                }
            }
        }
    }

    public byte[] getBloom(){
        return bloom;
    }

    public static void main(String[] args){
        try {
            Bloom bloom = new Bloom(200 * 1024);

            int count = 0;
            for(String file: args) {
                bloom.loadFile(new File(file));
                count++;
                System.out.printf("%d/%d\r", count, args.length);
            }
            System.out.println();

            new FileOutputStream("bloom.bin").write(bloom.getBloom());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
