package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.JsonParser;
import co.empathy.academy.search.util.JsonReference;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.JsonObject;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@Tag(name = "Index controller", description = "Allows the creation and deletion of indexes, as well as document indexing for certain," +
        "locally stored TSV with all the information to be used in the subsequent searches to ElasticClient")
@RestController
public class IndexController {

    /**
     * This method answers a get petition to index the document. Firstly it creates an index, then it applies a
     * mapping an finally indexes all the documents contained in the films .tsv.
     */
    @GetMapping("/index_documents")
    @ApiResponse(responseCode = "200", description = "Mapping done", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "answers a get petition to index the document. Firstly it creates an index, then it applies a" +
            " mapping an finally indexes all the documents contained in the films .tsv, whose path must be provided via get" +
            " parameter.")
    public void indexDocuments() {
        try {

            Thread bulkOperationTask = new Thread() {
                public void run() {
                    bulkOperations("/Users/danibaig/Desktop/academy/imdb-academy/imdb-academy/src/main/resources/static/title.basics.tsv");
                }
            };

            ClientCustomConfiguration.getClient().indices().delete(i -> i.index("films")); //TODO delete this

            ClientCustomConfiguration.getClient().indices().create(i -> i.index("films"));

            ClientCustomConfiguration.getClient().indices().putMapping(_0 -> _0.index("films")
                    .properties("titleType", _1 -> _1
                            .keyword(_2 -> _2))
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
                            .keyword(_2 -> _2))
            );

            bulkOperationTask.start(); //Starts the bulk operation thread so navigator won't get stuck without response

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * This method answers a petition to create an index in ElasticClient. It receives an indexName, necessary to create
     * the index
     * @param indexName
     * @param jsonDetails
     * @return boolean stating if everything is correct.
     */
    @ApiResponse(responseCode = "200", description = "Index created", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Creates an index with the specified name")
    @PutMapping("/{indexName}")
    public boolean createIndex(@PathVariable String indexName, @RequestBody String jsonDetails) {

        Reader queryJson = new StringReader(jsonDetails);
        var req = CreateIndexRequest.of(b -> b.index(indexName).withJson(queryJson));

        try {
            boolean created = ClientCustomConfiguration.getClient().indices().create(req).acknowledged();
            return created;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * This method answers to a delete petition. It deletes an index given its name.
     * @param indexName
     * @return boolean with the status of the index deletion
     */
    @ApiResponse(responseCode = "200", description = "Index deleted", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Deletes an index given its name")
    @DeleteMapping("/delete/{indexName}")
    public boolean removeIndex(@PathVariable String indexName) {

        try {

            var indices = ClientCustomConfiguration.getClient().indices().delete(i -> {
                return i.index(indexName);
            });

            return indices.acknowledged();

        } catch (IOException i ) {
            System.out.println(i.getStackTrace());
            return false;
        }
    }

    private void bulkOperations(String tsvPath) {
        final int BULK_OPERATIONS = 250000;

        List<String> parsedDocument = null;
        try {
            parsedDocument = Files.readAllLines(Path.of(tsvPath));
            System.out.println("Document read");
        } catch (IOException e) {
            e.printStackTrace();
        }

        JsonParser jParser = new JsonParser(parsedDocument.get(0).split("\t"));

        int documentLength = parsedDocument.size();

        List<JsonReference> subset;
        int documentsIndexed = 0;
        int lastIndex = 1;

        while(documentsIndexed < documentLength) {

            int newIndex = lastIndex + BULK_OPERATIONS;
            boolean lastBatch = false;

            if(newIndex > (documentLength - 1)) {
                newIndex = documentLength - 1;
                lastBatch = true;
            }

            subset = parsedDocument.subList(lastIndex, newIndex)
                    .stream().map(jParser::parse).toList();

            if(BULK_OPERATIONS / subset.size() == 1.0 || lastBatch) {
                launchBulk(subset);
                lastIndex += BULK_OPERATIONS;
                documentsIndexed += BULK_OPERATIONS;
            }

        }
    }

    private void launchBulk(List<JsonReference> subset) {
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = ClientCustomConfiguration.getClient().bulk(_0 -> _0
                    .operations(subset.stream().map(_1 -> BulkOperation.of(
                            _2 -> _2.index(_3 -> _3.index("films")
                                    .id(_1.getId())
                                    .document(_1.getJson()))
                    )).toList()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done bulk");
    }

}
