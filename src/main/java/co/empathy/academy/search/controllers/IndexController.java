package co.empathy.academy.search.controllers;

import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.JsonParser;
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
                ClientCustomConfiguration.getClient().create(b -> b.index("films")
                        .id(String.valueOf(j.get("tconst")))
                        .withJson(new StringReader(j.toString())));

            }

            r.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }



    }
}
