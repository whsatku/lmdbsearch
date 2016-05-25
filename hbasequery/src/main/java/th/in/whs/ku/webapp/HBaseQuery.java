package th.in.whs.ku.webapp;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;

import java.io.IOException;

public class HBaseQuery {
    public static void main(String[] args) {
        HBaseQueryFacade facade = null;
        try {
            facade = new HBaseQueryFacade();
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return;
        }

        HBaseQueryFacade.FacadeResult result = facade.query(args[0]);
        if(result == null){
            System.out.println("Not found");
            return;
        }
        System.out.printf("%s %d\n", result.filename, result.paragraph);
    }
}
