package co.empathy.academy.search.util.indexutils;

import co.empathy.academy.search.util.indexutils.converters.StringToDoubleConverter;
import co.empathy.academy.search.util.indexutils.converters.StringToIntConverter;
import jakarta.json.JsonObjectBuilder;

import java.util.List;

public class Rating {
    private static final int AVERAGE_RATING = 1;
    private static final int NUM_VOTES = 2;

    private Rating(){}

    public static void addRating(String line, JsonObjectBuilder builder, List<String> headers) {
        String[] fields = line.split("\t");

        builder.add(headers.get(AVERAGE_RATING), StringToDoubleConverter.getDouble(fields[AVERAGE_RATING]))
                .add(headers.get(NUM_VOTES), StringToIntConverter.getInt(fields[NUM_VOTES]));
    }
}
