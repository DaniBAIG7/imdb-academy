package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.search.exception.ElasticsearchConnectionException;
import co.empathy.academy.search.exception.IndexNotFoundException;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.json.Json;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@Tag(name = "Query controller", description = "Contains some queries to be thrown to ElasticSearch")
@RestController
@RequestMapping(value = "/api")
public class QueryController {

    @GetMapping("/terms/{index}/_search")
    @Parameter(name="index", description="Name of the index over which to throw the terms query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Terms query result", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a terms query for a given index. Requires a field and several terms to match it.")
    public List<Map<String, Object>> termsQuery(@PathVariable String index, @RequestParam String field, @RequestParam String values) throws ElasticsearchConnectionException, IndexNotFoundException {
        String[] valuesArray = values.split(",");
        var fieldValues = Arrays.stream(valuesArray).map(v -> FieldValue.of(v)).toList();
        var q = QueryBuilders.terms().field(field).terms(TermsQueryField.of(t -> t.value(fieldValues))).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/term/{index}/_search")
    @Parameter(name="index", description="Name of the index over which to throw the term query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Terms query result", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a term query for a given index. Requires a field and a term to match it.")
    public List<Map<String, Object>> termQuery(@PathVariable String index, @RequestParam String field, @RequestParam String value) throws ElasticsearchConnectionException, IndexNotFoundException {
        var q = QueryBuilders.term().field(field).value(value).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/multimatch/{index}/_search")
    @Parameter(name="index", description="Name of the index over which to throw the multimatch query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Multimatch query result", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a multimatch query for a given index. Requires several fields and a value to match them.")
    public List<Map<String, Object>> multiMatchQuery(@PathVariable String index, @RequestParam String fields, @RequestParam String value) throws ElasticsearchConnectionException, IndexNotFoundException {
        String[] fieldsArray = fields.split(",");
        var q = QueryBuilders.multiMatch().fields(Arrays.stream(fieldsArray).toList()).query(value).build();
        return launchQuery(new Query(q), index);
    }

    
    @GetMapping("/search")
    @Parameter(name="q", description="Allows creating a must-match query over the \"primaryTitle\" value from the database according to the provided value.", required = false)
    @Parameter(name="type", description="Allows creating a filter for the query over the \"type\" field.", required = false)
    @Parameter(name="genre", description="Allows creating a filter for the query over the \"genre\" field.", required = false)
    @Parameter(name="agg", description="Allows providing a certain field to perform an aggregation over it and define it as query result.", required = false)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Search query result", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = { @Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = { @Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a bool query combining the different parameters")
    public String aggFilterQuery(@RequestParam(required = false) Optional<String> q,
                                 @RequestParam(required = false) Optional<List<String>> type,
                                 @RequestParam(required = false) Optional<List<String>> genre,
                                 @RequestParam(required = false) Optional<String> gte,
                                 @RequestParam(required = false, name = "agg") Optional<String> aggField,
                                 @RequestParam(required = false) Optional<Integer> from,
                                 @RequestParam(required = false) Optional<Integer> size
                                 ) throws ElasticsearchConnectionException, IndexNotFoundException {
        SearchRequest req = SearchRequest.of(indexRequest -> {

            if(from.isPresent())
                indexRequest.from(from.get());
            if(size.isPresent())
                indexRequest.size(size.get());

            var wholeQuery = QueryBuilders.bool();

            if(q.isPresent()) {

                wholeQuery.must(_1 -> _1
                        .multiMatch(_2 -> _2
                                .fields("primaryTitle", "originalTitle", "startYear^2")
                                .operator(Operator.And)
                                .fuzziness("2")
                                .analyzer("simple")
                                .type(TextQueryType.BestFields)
                                .query(q.get())
                                .tieBreaker(0.3)

                        )
                );
            }

            if(type.isPresent())
                putFilter(type.get(), "titleType", wholeQuery);
            if(genre.isPresent())
                putFilter(genre.get(), "genres", wholeQuery);
            if(gte.isPresent())
                wholeQuery.filter(_1 -> _1.range(_2 ->
                        _2.field("averageRating").gte(JsonData.of(gte.get()))));

            var functionQuery = QueryBuilders.functionScore();
            functionQuery.query(new Query(wholeQuery.build())).functions(FunctionScore.of(function ->
                    function.scriptScore(
                            scrScoreFn -> scrScoreFn.script(
                                    Script.of(script -> script.inline(
                                            inlineScr -> inlineScr.source(
                                                    "Math.log(2 + doc['averageRating'].value) * doc['numVotes'].value"
                                            )
                                    )))
                    )
            ));

            //Assigning query to films index and placing it into request
            var wholeReq = indexRequest.index("films").query(new Query(functionQuery.build()));

            if(aggField.isPresent()) {
               wholeReq.aggregations(
                       aggField.get() + "_agg",
                       AggregationBuilders.terms().field(aggField.get()).build()._toAggregation()
               );
            }


            return wholeReq;
        });

        try {
            var response = ClientCustomConfiguration.getClient().search(req, JsonData.class);
            return getResultsAsString(aggField, response);
        } catch (IOException e) {
            throw new ElasticsearchConnectionException(e);
        } catch (ElasticsearchException i) {
            throw new IndexNotFoundException("films", i);
        }


    }

    private void putFilter(List<String> values, String field, BoolQuery.Builder builder) {
        builder.filter(_1 -> _1.terms(_2 -> _2
                        .field(field).terms(_3 -> _3
                                .value(values.stream().map(FieldValue::of).toList())
                        )
                ) //Parseo de los typeFilters a FieldValue
        );
    }

    private List<Map<String, Object>> launchQuery(Query q, String index) {
        SearchRequest searchRequest = new SearchRequest.Builder().query(q).index(index).build();

        try {
            SearchResponse<JsonData> res = ClientCustomConfiguration.getClient().search(searchRequest, JsonData.class);
            return getResults(res);
        } catch (IOException e) {
            throw new ElasticsearchConnectionException(e);
        } catch (ElasticsearchException i) {
            throw new IndexNotFoundException(index, i);
        }
    }


    /**
     * Private method that manages response for the term, terms and multimatch queries
     * @param response
     * @return a json with the response
     */
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

    public String getResultsAsString(Optional<String> aggFieldOpt, SearchResponse<JsonData> response) {


        String toRet;

        if(aggFieldOpt.isPresent()) {
            toRet = parseAggregations(aggFieldOpt.get(), response);
        } else {
            toRet = response.hits().hits().stream()
                    .filter(x -> x.source() != null)
                    .map(x -> Json.createObjectBuilder()
                            .add("id", x.id())
                            .add("source", x.source().toJson())
                            .build()).toList().toString();
        }

        return toRet;

    }

    private String parseAggregations(String aggField, SearchResponse<JsonData> response) {
        String aggName = aggField + "_agg";
        var list = response.aggregations().get(aggName).sterms().buckets().array();

        var buckets = list.stream()
                .map(x -> Json.createObjectBuilder().add("key", x.key()).add("doc_count", x.docCount()).build());

        var result = Json.createArrayBuilder();

        buckets.forEach(result::add);

        return result.build().toString();
    }


}
