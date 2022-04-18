package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

@Tag(name = "Document controller", description = "Allows the creation, update and deletion of documents")
@RestController
public class CrudDocumentController {

    @PostMapping("/{indexName}/_doc")
    @ApiResponse(responseCode = "200", description = "Document created", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Creates a document on an index given its id by get parameter, according ot a Json received in the petition" +
            "body.")
    public void createDocument(@PathVariable String indexName, @RequestParam(required = false) String id, @RequestBody String docJson) {
        try {

            Reader queryJson = new StringReader(docJson);

            ClientCustomConfiguration.getClient().create(i -> {
                var indexReq= i.index(indexName).withJson(queryJson);

                if(id != null) {
                    indexReq.id(id);
                }
                return indexReq;
            });

        } catch (IOException i ) {
            System.out.println(i.getStackTrace());
        }
    }

    @ApiResponse(responseCode = "200", description = "Document updated", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Updates a document with its id and index provided via path value, according to a Json body given by " +
            "request body value")
    @PutMapping("/{indexName}/_doc/{id}")
    public void updateDocument(@PathVariable String indexName, @PathVariable String id, @RequestBody Map<String, Object> json) {

        try {

            var req = UpdateRequest.of(q -> q.index(indexName).id(id).doc(json));

            ClientCustomConfiguration.getClient().update(req, Object.class);
        } catch (IOException i ) {
            System.out.println(i.getStackTrace());
        }
    }


}
