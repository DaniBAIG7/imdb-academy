package co.empathy.academy.search.util.indexutils.converters;

public class StringToIntConverter {

    public static int getInt(String s) {
        if(s.equals("\\N"))
            return 0;
        return Integer.parseInt(s);
    }
}
