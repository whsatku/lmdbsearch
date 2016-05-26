package th.in.whs.ku.webapp.server;

import com.google.common.base.Charsets;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;
import com.google.common.io.Resources;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.rapidoid.http.Req;
import org.rapidoid.http.ReqHandler;
import org.rapidoid.setup.On;
import th.in.whs.ku.webapp.HBaseQueryFacade;
import th.in.whs.ku.webapp.common.Tokenize;

import java.io.IOException;
import java.util.List;

public class HBaseServer {
    private static String html;
    private static HBaseQueryFacade facade;

    public static void main(String[] args){
        try {
            html = Resources.toString(Resources.getResource("fast.html"), Charsets.UTF_8).replaceAll("[\n\r\t]", "");
        } catch (IOException e) {
            html = "%OUTPUT%";
        }
        try {
            facade = new HBaseQueryFacade();
        } catch (IOException | ServiceException e) {
            e.printStackTrace();
            return;
        }

        On.port(9999);

        On.get("/").html(html.replace("%OUTPUT%", "").replace("%INPUT%", ""));
        On.post("/").html(new ReqHandler() {
            public Object execute(Req req) throws Exception {
                String input = req.posted("input").toString();
                List<String> tokens = Tokenize.tokenize(input);
                HBaseQueryFacade.FacadeResult[] results = query(input);

                StringBuilder output = new StringBuilder();
                Escaper escaper = HtmlEscapers.htmlEscaper();

                for(int i = 0; i < results.length; i++){
                    if(results[i] == null){
                        output.append("<div class=\"red\">");
                        output.append(escaper.escape(tokens.get(i)));
                        output.append("</div>");
                    }else{
                        output.append("<div class=\"green\">");
                        output.append(escaper.escape(tokens.get(i)));
                        output.append("<span class=\"found\">");
                        output.append(escaper.escape(results[i].filename));
                        output.append("#");
                        output.append(results[i].paragraph);
                        output.append("</span></div>");
                    }
                }

                return html.replace("%INPUT%", escaper.escape(input))
                        .replace("%OUTPUT%", output.toString());
            }
        });

        On.post("/api").json(new ReqHandler() {
            public Object execute(Req req) throws Exception {
                return query(req.posted("input").toString());
            }
        });
    }



    private static HBaseQueryFacade.FacadeResult[] query(String str){
        List<String> input = Tokenize.tokenize(str);
        HBaseQueryFacade.FacadeResult[] out = new HBaseQueryFacade.FacadeResult[input.size()];

        for(int i = 0; i < input.size(); i++){
            out[i] = facade.query(input.get(i));
        }

        return out;
    }
}
