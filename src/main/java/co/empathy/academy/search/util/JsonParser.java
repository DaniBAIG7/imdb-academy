package co.empathy.academy.search.util;


import jakarta.json.Json;
import jakarta.json.JsonObject;

import java.util.Map;


public class JsonParser {

    private String[] fields;

    /**
     *  The constructor of the class stores the fields of the Json that we want to be parsed
     * @param fields of all TSV documents for parsing the Json
     */
    public JsonParser(String... fields) {
        this.fields = fields;
    }

    /**
     * This method allows parsing a Json taking as reference one line of the TSV
     * @param line of the document (tsv) to be parsed to a Json
     * @return a map that bounds a String (key of the document) with a whole Json to be added to the document
     */
    public Map<String, JsonObject> of(String line) {
        String[] jsonFields = line.split("\t");

        var builder = Json.createObjectBuilder();

        for(int i = 1; i < fields.length; i++) {
            if(i == 4) { //isAdult field
                boolean toAdd = Integer.parseInt(jsonFields[i]) == 1;
                builder.add(fields[i], toAdd);
            }
            else if(i == 5 || i == 6 || i == 7) { //startYear, endYear or runtimeMinutes field
                if(jsonFields[i].equals("\\N")) { //If they are not integers, put 0
                    builder.add(fields[i], 0);
                } else {
                    builder.add(fields[i], jsonFields[i]);
                }
            }
            else {
                builder.add(fields[i], jsonFields[i]);
            }
        }

        return Map.of(jsonFields[0], builder.build());
    }
}
