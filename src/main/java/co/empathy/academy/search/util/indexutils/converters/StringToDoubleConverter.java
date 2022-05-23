package co.empathy.academy.search.util.indexutils.converters;

public class StringToDoubleConverter {

    public static double getDouble(String s) {
        if(s.equals("\\N"))
            return 0;
        return Double.parseDouble(s);
    }
}
