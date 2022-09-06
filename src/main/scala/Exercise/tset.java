package Exercise;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

public class tset {
    public static void main(String[] args) throws Exception {
        BufferedReader r = new BufferedReader (new FileReader("data/exercise/test01.txt"));
        String s = r.readLine();
        String[] split = s.split(",");
        for (String s1 : split) {
            //System.out.println(s1);//{"oid":"o123", "cid": 1, "money": 600.0, "longitude":116.397128,"latitude":39.916527}
            String[] split1 = s1.split(":");
            for (String s2 : split1) {
                System.out.println(s2);
            }
        }

    }
}
