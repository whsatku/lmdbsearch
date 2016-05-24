package th.in.whs.ku.webapp.server;

import com.google.common.base.Charsets;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;
import com.google.common.io.Resources;
import org.rapidoid.http.Req;
import org.rapidoid.http.ReqHandler;
import org.rapidoid.setup.On;
import th.in.whs.ku.webapp.common.QueryFacade;
import th.in.whs.ku.webapp.lmdb.LmdbQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FastServer {
    private static String html;

    public static void main(String[] args){
        try {
            html = Resources.toString(Resources.getResource("fast.html"), Charsets.UTF_8).replaceAll("[\n\r\t]", "");
        } catch (IOException e) {
            html = "%OUTPUT%";
        }
        On.get("/").html(html.replace("%OUTPUT%", "").replace("%INPUT%", ""));
        On.post("/").html(new ReqHandler() {
            public Object execute(Req req) throws Exception {
                String input = req.posted("input").toString();
                List<String> tokens = tokenize(input);
                LmdbQuery.Result[] results = query(input);

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
                        output.append(escaper.escape(results[i].file));
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

    private static List<String> tokenize(String str){
        List<String> output = new ArrayList<>();
        str = str.replace("\r\n", "\n");

        for(String lines: str.split("\n\n")){
            String[] sentences = lines.replace("\n", " ").split("\\. ");
            for(String sentence: sentences){
                output.add(sentence.trim());
            }
        }

        return output;
    }

    private static LmdbQuery.Result[] query(String str){
        List<String> input = tokenize(str);
        LmdbQuery.Result[] out = new LmdbQuery.Result[input.size()];

        for(int i = 0; i < input.size(); i++){
            out[i] = QueryFacade.query(input.get(i));
        }

        return out;
    }
}
