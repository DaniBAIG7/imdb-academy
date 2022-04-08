package co.empathy.academy.search.util;


import jakarta.json.Json;
import jakarta.json.JsonObject;


public class JsonParser {

    private String[] fields;

    public JsonParser(String... fields) {
        this.fields = fields;
    }

    public JsonObject of(String line) {
        String[] jsonFields = line.split("\t");

        var builder = Json.createObjectBuilder();

        for(int i = 0; i < fields.length; i++) {
            builder.add(fields[i], jsonFields[i]);
        }

        return builder.build();
    }
}
