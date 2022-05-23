package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.empathy.academy.search.exception.*;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import co.empathy.academy.search.util.indexutils.BatchReader;
import co.empathy.academy.search.util.indexutils.IndexingUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Tag(name = "Index controller", description = "Allows the creation and deletion of indexes, as well as document indexing for certain," +
        "locally stored TSV with all the information to be used in the subsequent searches to ElasticClient.")
@RestController
@RequestMapping(value="/admin/api")
public class IndexController {
    private static final Logger logger = LoggerFactory.getLogger(IndexController.class);

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
                               @RequestParam String ratingsPath,
                               @RequestParam String akasPath,
                               @RequestParam String crewPath,
                               @RequestParam String episodesPath,
                               @RequestParam String principalPath,
                               @RequestParam String nameBasicsPath) {
        try {

            Thread bulkOperationTask = new Thread() {
                public void run() {
                    bulkOperations(filmsPath, ratingsPath, akasPath, crewPath, episodesPath, principalPath, nameBasicsPath);
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

            ClientCustomConfiguration.getClient().indices().close(_0 -> _0.index("films"));

            var analyzer = getClass().getClassLoader().getResourceAsStream("analyzers.json");
            ClientCustomConfiguration.getClient().indices().putSettings(_0 -> _0.index("films").withJson(analyzer));

            ClientCustomConfiguration.getClient().indices().open(_0 -> _0.index("films"));

            var mapping = getClass().getClassLoader().getResourceAsStream("mappings.json");
            ClientCustomConfiguration.getClient().indices().putMapping(_0 -> _0.index("films").withJson(mapping));


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

    private void bulkOperations(String filmsPath,
                                String ratingsPath,
                                String akasPath,
                                String crewPath,
                                String episodesPath,
                                String principalPath,
                                String nameBasicsPath) {

        int batchSize = 25000;

        try {
            logger.info("Started indexing");
            var batchReader = new BatchReader(filmsPath, ratingsPath, akasPath, crewPath,
                    episodesPath, principalPath, nameBasicsPath, batchSize);

            while(!batchReader.hasFinished()) {
                var batch = batchReader.getBatch();

                ClientCustomConfiguration.getClient().bulk(bulkRequest -> bulkRequest
                        .operations(batch.stream()
                                .map(x ->
                                        BulkOperation.of(_1 -> _1
                                                .index(_2 -> _2
                                                        .index("films")
                                                        .document(x.json())
                                                        .id(x.id())
                                                )
                                        )
                                ).toList())
                );

                logger.info("Done bulk");
            }

            logger.info("Indexed");
            batchReader.close();
        } catch(IOException | ElasticsearchException e) {
            throw new InternalServerException(e);
        }
    }


}
