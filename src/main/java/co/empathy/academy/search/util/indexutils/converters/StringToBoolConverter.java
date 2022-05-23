package co.empathy.academy.search.util.indexutils.converters;

public class StringToBoolConverter {

    public static boolean getBool(String s) {
        return s.equals("1");
    }
}
