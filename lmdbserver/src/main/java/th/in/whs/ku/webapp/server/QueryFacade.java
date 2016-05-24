package th.in.whs.ku.webapp.server;

import th.in.whs.ku.webapp.bloom.BloomQuery;
import th.in.whs.ku.webapp.lmdb.LmdbQuery;

public class QueryFacade {
    private static final LmdbQuery checker = new LmdbQuery("db/text", "db/file");
    private static final BloomQuery bloom = new BloomQuery("bloom.bin");

    public static LmdbQuery.Result query(String input){
        if(!bloom.query(input)){
            return null;
        }

        return checker.check(input);
    }
}
