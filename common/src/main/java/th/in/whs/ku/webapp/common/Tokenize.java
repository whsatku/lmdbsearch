package th.in.whs.ku.webapp.common;

import java.util.ArrayList;
import java.util.List;

public class Tokenize {
    public static List<String> tokenize(String str){
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
}
