package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.mapping.BooleanProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.JsonParser;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;

@RestController
public class IndexController {

    @PutMapping("/index_documents")
    public void indexDocuments() {
        try {
            ClientCustomConfiguration.getClient().indices().delete(i -> {
                return i.index("films");
            });

            BufferedReader r = new BufferedReader(new FileReader("src/main/resources/static/title.basics.tsv"));
            JsonParser jParser = new JsonParser(r.readLine().split("\t"));
            String currentLine;

            while ((currentLine = r.readLine()) != null) {
                JsonObject j = jParser.of(currentLine);
                var req = CreateIndexRequest.of(b -> b.index("films")
                        .mappings(_0 -> _0
                                .properties("titleType", _1 -> _1
                                        .text(_2 -> _2))
                                .properties("primaryTitle", _1 -> _1
                                        .text(_2 -> _2
                                                .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4))
                                        ))
                                .properties("originalTitle", _1 -> _1
                                        .text(_2 -> _2
                                                .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4))
                                        ))
                                .properties("isAdult", _1 -> _1
                                        .boolean_(_2 -> _2))
                                .properties("startYear", _1 -> _1
                                        .integer(_2 -> _2.index(false)))
                                .properties("endYear", _1 -> _1
                                        .integer(_2 -> _2))
                                .properties("runtimeMinutes", _1 -> _1
                                        .integer(_2 -> _2.index(false)))
                                .properties("genres", _1 -> _1
                                        .text(_2 -> _2.analyzer("english"))))
                );



            }

            r.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
