package th.in.whs.ku.webapp.common;

import com.google.common.primitives.Longs;
import net.openhft.hashing.LongHashFunction;

public class Hasher {
    private static final LongHashFunction HASH = LongHashFunction.xx_r39();

    public static byte[] hash(String str){
        return Longs.toByteArray(HASH.hashChars(str));
    }
}
