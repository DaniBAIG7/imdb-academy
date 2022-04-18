package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.TsvReader;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            " mapping an finally indexes all the documents contained in the films .tsv.")
    public void indexDocuments() {
        try {

            ClientCustomConfiguration.getClient().indices().delete(i -> i.index("films")); //TODO delete this

            ClientCustomConfiguration.getClient().indices().create(i -> i.index("films"));

            ClientCustomConfiguration.getClient().indices().putMapping(_0 -> _0.index("films")
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
                            .text(_2 -> _2.analyzer("english")))
            );

            TsvReader r = new TsvReader();

            while (r.isOpen()) {//The custom TSVReader has not read a

                var a = ClientCustomConfiguration.getClient().bulk(_0 -> _0
                        .operations(r.readSeveral().entrySet().stream().map(_1 -> BulkOperation.of(
                                _2 -> _2.index(_3 -> _3.index("films")
                                        .id(_1.getKey())
                                        .document(_1.getValue()))
                        )).toList()));


                    if(a.errors()) {
                      for(var d: a.items()) {
                        if(d.error() != null)
                            System.out.println(d.error().reason());
                    }
                }
            }


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

}
