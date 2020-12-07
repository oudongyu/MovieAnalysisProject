package demo;

import com.bigdata.movies.MRAnalysis;
import org.apache.commons.lang.StringUtils;

public class test {
    public static void main(String[] args) {
        /*String a="aa\t\t\t\t \t\t\t\t";
        String[] strings = a.split("\t");
        for (String string : strings) {
            System.out.println(string);
        }
        System.out.println(strings.length);*/
//        String aa="6892";
//        String strip = StringUtils.strip(aa, "人评价)(");
//        System.out.println(strip);

        System.out.println(new MRAnalysis().showingDateFormat("1989(台湾)"));
    }
}
