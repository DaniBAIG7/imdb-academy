package co.empathy.academy.search.util;


import co.empathy.academy.search.exception.NoRatingsException;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;

import java.util.Map;
import java.util.Optional;


public class JsonParser {

    private String[] filmsFields;
    private Optional<String[]> ratingsFieldsOpt;


    /**
     * The constructor of the class stores the fields of the Json that we want to be parsed
     * @param filmsFields the fields of the films document to be parsed
     * @param ratingsFields (optional) the fields of the ratings document to be parsed
     */
    public JsonParser(String[] filmsFields, Optional<String[]> ratingsFields) {
        this.filmsFields = filmsFields;
        if(ratingsFields.isPresent()) {
            this.ratingsFieldsOpt = Optional.of(ratingsFields.get());
        } else {
            this.ratingsFieldsOpt = Optional.empty();
        }
    }

    /**
     * This method allows parsing a Json taking as reference one line of films.tsv
     * @param line of the document (tsv) to be parsed to a Json
     * @return a map that bounds a String (key of the document) with a whole Json to be added to the document
     */
    public JsonReference parse(String line) {
        String[] filmJsonFields = line.split("\t");

        var builder = parseFilmsBuilder(filmJsonFields);

        return new JsonReference(filmJsonFields[0], builder.build());
    }

    private JsonObjectBuilder parseFilmsBuilder(String[] jsonFields) {
        var builder = Json.createObjectBuilder();

        for(int i = 1; i < filmsFields.length; i++) {
            if(i == 4) { //isAdult field
                boolean toAdd = Integer.parseInt(jsonFields[i]) == 1;
                builder.add(filmsFields[i], toAdd);
            }
            else if(i == 5 || i == 6 || i == 7) { //startYear, endYear or runtimeMinutes field
                if(jsonFields[i].equals("\\N")) { //If they are not integers, put 0
                    builder.add(filmsFields[i], 0);
                } else {
                    builder.add(filmsFields[i], jsonFields[i]);
                }
            }
            else if(i == 8) { //Genres field
                var arrayBuilder = Json.createArrayBuilder();
                for(String s: jsonFields[i].split(",")) {
                    arrayBuilder.add(s);
                }
                builder.add(filmsFields[i], arrayBuilder.build());
            }
            else {
                builder.add(filmsFields[i], jsonFields[i]);
            }
        }

        return builder;
    }

    /**
     * This method parses the same line from two TSV documents and combines them into a single JsonReference
     * @param line1 a Line of the title.basics.tsv
     * @param line2 a line of title.ratings.tsv
     * @return a JsonReference result of the combination of the two TSV lines
     * @throws NoRatingsException
     */
    public JsonReference biParse(String line1, Optional<String> line2) throws NoRatingsException {
        if(ratingsFieldsOpt.isEmpty()) {
            throw new NoRatingsException();
        }

        String[] filmsJsonFields = line1.split("\t");

        //Parseo de la parte de films
        var filmsBuilder = parseFilmsBuilder(filmsJsonFields);

        String[] ratingsFields = ratingsFieldsOpt.get();
        if(line2.isEmpty()) {
            filmsBuilder.add(ratingsFields[1], "0");
            filmsBuilder.add(ratingsFields[2], "0");
        } else {
            String[] ratingsJsonFields = line2.get().split("\t");
            for(int i = 1; i < ratingsFields.length; i++) {
                filmsBuilder.add(ratingsFields[i], ratingsJsonFields[i]);
            }
        }

        //Combinación de los dos ratings
        return new JsonReference(filmsJsonFields[0], filmsBuilder.build());
    }

}
