package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.empathy.academy.search.exception.ElasticsearchConnectionException;
import co.empathy.academy.search.exception.IndexAlreadyExistsException;
import co.empathy.academy.search.exception.IndexNotFoundException;
import co.empathy.academy.search.exception.NoRatingsException;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.JsonParser;
import co.empathy.academy.search.util.JsonReference;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.JsonObject;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@Tag(name = "Index controller", description = "Allows the creation and deletion of indexes, as well as document indexing for certain," +
        "locally stored TSV with all the information to be used in the subsequent searches to ElasticClient.")
@RestController
@RequestMapping(value="/admin/api")
public class IndexController {

    /**
     * This method answers a get petition to index the document. Firstly it creates an index, then it applies a
     * mapping an finally indexes all the documents contained in the films .tsv.
     */
    @GetMapping("/index_documents")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Mapping done", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index not found", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Parameters({
            @Parameter(name = "ratingsPath", description = "Path for ratings.tsv", required = true),
            @Parameter(required = false, name = "filmsPath", description = "Path for films.tsv")
    })
    @Operation(summary = "answers a get petition to index the document. Firstly, it creates the index (deleting it if already exists), then it applies a" +
            " mapping an finally indexes to \"films\" index all the documents contained in the films .tsv (and optionally the ratings .tsv)," +
            " whose paths must be provided via get parameter.")
    public void indexDocuments(@RequestParam String filmsPath,
                               @RequestParam(name = "ratingsPath", required = false) Optional<String> ratingsPathOpt) {
        try {

            Thread bulkOperationTask = new Thread() {
                public void run() {
                    indexOperations(filmsPath, ratingsPathOpt);
                }
            };

            try {
                ClientCustomConfiguration.getClient().indices().delete(i -> {
                    return i.index("films");
                });
                ClientCustomConfiguration.getClient().indices().create(i -> i.index("films"));
            } catch(ElasticsearchException e) {
                ClientCustomConfiguration.getClient().indices().create(i -> i.index("films"));
            }

            ClientCustomConfiguration.getClient().indices().putMapping(_0 -> {
                var req = _0.index("films");
                        req.properties("titleType", _1 -> _1
                                .keyword(_2 -> _2))
                        .properties("primaryTitle", _1 -> _1
                                .text(_2 -> _2.boost(8.0)
                                        .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4.boost(9.0)))
                                ))
                        .properties("originalTitle", _1 -> _1
                                .text(_2 -> _2.boost(9.8)
                                        .analyzer("standard").fields("raw", _3 -> _3.keyword(_4 -> _4.boost(10.0)))
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
                                .keyword(_2 -> _2));

                if(ratingsPathOpt.isPresent())  {
                    req.properties("averageRating", _1 -> _1
                                    .double_(_2 -> _2.index(false)))
                        .properties("numVotes", _1 -> _1
                            .integer(_2 -> _2.index(false)));
                }

                return req;

                }
            );

            bulkOperationTask.start(); //Starts the bulk operation thread so navigator won't get stuck without response

        } catch (IOException i) {
            throw new ElasticsearchConnectionException(i);
        } catch (ElasticsearchException e) {
            throw new IndexNotFoundException("films", e);
        }

    }

    /**
     * This method answers a petition to create an index in ElasticClient. It receives an indexName, necessary to create
     * the index
     * @param indexName
     * @param jsonDetails
     * @return boolean stating if everything is correct.
     */
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Index created", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index already exists", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Parameter(name="indexName", description="Name of the index to be created")
    @Operation(summary = "Creates an index with the specified name")
    @PutMapping("/{indexName}")
    public boolean createIndex(@PathVariable String indexName, @RequestBody String jsonDetails) {

        Reader queryJson = new StringReader(jsonDetails);
        var req = CreateIndexRequest.of(b -> b.index(indexName).withJson(queryJson));

        try {
            return ClientCustomConfiguration.getClient().indices().create(req).acknowledged();
        } catch (IOException i) {
            throw new ElasticsearchConnectionException(i);
        } catch (ElasticsearchException e) {
            throw new IndexAlreadyExistsException(indexName, e);
        }
    }


    /**
     * This method answers to a delete petition. It deletes an index given its name.
     * @param indexName
     * @return boolean with the status of the index deletion
     */
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Index deleted", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index already exists", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Operation(summary = "Deletes an index given its name")
    @DeleteMapping("/delete/{indexName}")
    public boolean removeIndex(@PathVariable String indexName) {

        try {

            var indices = ClientCustomConfiguration.getClient().indices().delete(i -> {
                return i.index(indexName);
            });

            return indices.acknowledged();

        } catch (IOException i ) {
            throw new ElasticsearchConnectionException(i);
        } catch (ElasticsearchException e) {
            throw new IndexNotFoundException(indexName, e);
        }
    }

    /**
     * This method reads all the two different documents provided their paths; Then invokes bulkOperations to do the
     * parsing and launch the bulk.
     * @param filmsPath
     * @param ratingsPathOpt
     */
    private void indexOperations(String filmsPath, Optional<String> ratingsPathOpt) {
        List<String> parsedFilmsDocument = null;
        Optional<List<String>> parsedRatingsDocument = null;
        try {
            parsedFilmsDocument = Files.readAllLines(Path.of(filmsPath));
            System.out.println("Films document read");
            if(ratingsPathOpt.isPresent()) {
                parsedRatingsDocument = Optional.of(Files.readAllLines(Path.of(ratingsPathOpt.get())));
                System.out.println("Ratings document read");
                bulkOperations(parsedFilmsDocument, Optional.of(parsedRatingsDocument.get()));
            } else {
                bulkOperations(parsedFilmsDocument, Optional.empty());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void bulkOperations(List<String> filmsDocument, Optional<List<String>> ratingsDocument) {
        //I'm sorry for this method ;,)

        JsonParser jParser;
        Optional<Map<String, String>> ratingsMap;

        if(!ratingsDocument.isEmpty()) {
            jParser = new JsonParser(filmsDocument.get(0).split("\t"), Optional.of(ratingsDocument.get().get(0).split("\t")));
            Map<String, String> auxMap = new HashMap<String, String>();

            ratingsDocument.get().forEach((value) -> auxMap.put(value.split("\t")[0], value));
            System.out.println("Ratings parsed");

            ratingsMap = Optional.of(auxMap);
        } else {
            jParser = new JsonParser(filmsDocument.get(0).split("\t"), Optional.empty());
            ratingsMap = Optional.empty();
        }

        final int BULK_OPERATIONS = 25000;

        int documentLength = filmsDocument.size(); //In fact, any of the two documents could do since they have the same lines

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

            if(!ratingsDocument.isEmpty()) {
                List<JsonReference> auxSubset = new LinkedList<>();
                for(int i = lastIndex; i < newIndex; i++) {
                    try {
                        String id = filmsDocument.get(i).split("\t")[0];
                        auxSubset.add(jParser.biParse(filmsDocument.get(i),
                                Optional.ofNullable(ratingsMap.get().get(id))));
                    } catch (NoRatingsException e) {
                        e.printStackTrace();
                    }
                }
                subset = auxSubset;
            } else {
                subset = filmsDocument.subList(lastIndex, newIndex)
                        .stream().map(jParser::parse).toList();
            }


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
