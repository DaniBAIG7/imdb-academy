package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQueryField;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Tag(name = "Query controller", description = "Contains some queries to be thrown to ElasticSearch")
@RestController("/api")
public class QueryController {

    @ApiResponse(responseCode = "200", description = "Terms query result", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Throws a terms query for a given index. Requires a field and several terms to match it.")
    @GetMapping("/terms/{index}/_search")
    public List<Map<String, Object>> termsQuery(@PathVariable String index, @RequestParam String field, @RequestParam String values)  {
        String[] valuesArray = values.split(",");
        var fieldValues = Arrays.stream(valuesArray).map(v -> FieldValue.of(v)).toList();
        var q = QueryBuilders.terms().field(field).terms(TermsQueryField.of(t -> t.value(fieldValues))).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/term/{index}/_search")
    @ApiResponse(responseCode = "200", description = "Term query result", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Throws a term query for a given index. Requires a field and a term to match it.")
    public List<Map<String, Object>> termQuery(@PathVariable String index, @RequestParam String field, @RequestParam String value) {
        var q = QueryBuilders.term().field(field).value(value).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/multimatch/{index}/_search")
    @ApiResponse(responseCode = "200", description = "Multimatch query result", content = { @Content(mediaType = "application/json")})
    @Operation(summary = "Throws a multimatch query for a given index. Requires several fields and a value to match them.")
    public List<Map<String, Object>> multiMatchQuery(@PathVariable String index, @RequestParam String fields, @RequestParam String value) {
        String[] fieldsArray = fields.split(",");
        var q = QueryBuilders.multiMatch().fields(Arrays.stream(fieldsArray).toList()).query(value).build();
        return launchQuery(new Query(q), index);
    }



    private List<Map<String, Object>> launchQuery(Query q, String index) {
        SearchRequest searchRequest = new SearchRequest.Builder().query(q).index(index).build();

        try {
            SearchResponse<JsonData> res = ClientCustomConfiguration.getClient().search(searchRequest, JsonData.class);
            return getResults(res);
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>(); //If this happens query has failed
        }
    }

    private List<Map<String, Object>> getResults(SearchResponse<JsonData> response) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = new ArrayList<>();

        for(Hit<JsonData> h: response.hits().hits()) {
            Map<String, Object> m = null;
            try {
                m = mapper.readValue(h.source().toString(), Map.class);
            } catch (JsonProcessingException e ) {
                e.printStackTrace();
            }

            result.add(m);
        }

        return result;
    }

}
