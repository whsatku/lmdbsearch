package th.in.whs.ku.webapp.bloom;

import com.google.common.primitives.UnsignedLongs;
import net.openhft.hashing.LongHashFunction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BloomQuery {
    byte[] bloom;

    public BloomQuery(String file) {
        try {
            bloom = Files.readAllBytes(Paths.get(file));
        } catch (IOException e) {
            bloom = new byte[0];
        }
    }

    public boolean query(String str){
        if(bloom.length == 0){
            return true;
        }

        long[] hashes = {
                LongHashFunction.xx_r39().hashChars(str),
                LongHashFunction.murmur_3().hashChars(str),
        };

        for(long hash: hashes){
            int bucket = (int) (UnsignedLongs.divide(hash, 8) % bloom.length);
            byte bit = (byte) UnsignedLongs.remainder(hash, 8);
            if((bloom[bucket] & (1 << bit)) == 0){
                return false;
            }
        }

        return true;
    }
}
