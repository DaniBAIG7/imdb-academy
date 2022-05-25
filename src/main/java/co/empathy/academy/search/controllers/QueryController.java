package co.empathy.academy.search.controllers;

import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.empathy.academy.search.util.queryutils.HiClientOps;
import co.empathy.academy.search.util.queryutils.ResultParser;
import co.empathy.academy.search.exception.ElasticsearchConnectionException;
import co.empathy.academy.search.exception.IndexNotFoundException;
import co.empathy.academy.search.util.ClientCustomConfiguration;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@Tag(name = "Query controller", description = "Contains some queries to be thrown to ElasticSearch")
@RestController
@RequestMapping(value = "/api")
public class QueryController {

    @GetMapping("/terms/{index}/_search")
    @Parameter(name = "index", description = "Name of the index over which to throw the terms query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Terms query result", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = {@Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a terms query for a given index. Requires a field and several terms to match it.")
    public List<Map<String, Object>> termsQuery(@PathVariable String index, @RequestParam String field, @RequestParam String values) throws ElasticsearchConnectionException, IndexNotFoundException {
        String[] valuesArray = values.split(",");
        var fieldValues = Arrays.stream(valuesArray).map(FieldValue::of).toList();
        var q = QueryBuilders.terms().field(field).terms(TermsQueryField.of(t -> t.value(fieldValues))).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/term/{index}/_search")
    @Parameter(name = "index", description = "Name of the index over which to throw the term query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Terms query result", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = {@Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a term query for a given index. Requires a field and a term to match it.")
    public List<Map<String, Object>> termQuery(@PathVariable String index, @RequestParam String field, @RequestParam String value) throws ElasticsearchConnectionException, IndexNotFoundException {
        var q = QueryBuilders.term().field(field).value(value).build();
        return launchQuery(new Query(q), index);
    }

    @GetMapping("/multimatch/{index}/_search")
    @Parameter(name = "index", description = "Name of the index over which to throw the multimatch query")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Multimatch query result", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = {@Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a multimatch query for a given index. Requires several fields and a value to match them.")
    public List<Map<String, Object>> multiMatchQuery(@PathVariable String index, @RequestParam String fields, @RequestParam String value) throws ElasticsearchConnectionException, IndexNotFoundException {
        String[] fieldsArray = fields.split(",");
        var q = QueryBuilders.multiMatch().fields(Arrays.stream(fieldsArray).toList()).query(value).build();
        return launchQuery(new Query(q), index);
    }


    @GetMapping("/search")
    @Parameter(name = "q", description = "Allows creating a must-match query over the \"primaryTitle\" value from the database according to the provided value.", required = true)
    @Parameter(name = "type", description = "Allows creating a filter for the query over the \"type\" field.", required = false)
    @Parameter(name = "genre", description = "Allows creating a filter for the query over the \"genre\" field.", required = false)
    @Parameter(name = "agg", description = "Allows providing a certain field to perform an aggregation over it and define it as query result.", required = false)
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Search query result", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "400", description = "Index does not exist", content = {@Content(mediaType = "application/json")}),
            @ApiResponse(responseCode = "500", description = "Could not connect to Elasticsearch", content = {@Content(mediaType = "application/json")})
    })
    @Operation(summary = "Throws a query combining the different parameters, boosted depending on the rating and number of votes.")
    public String aggFilterQuery(@RequestParam String q,
                                 @RequestParam(required = false) Optional<List<String>> type,
                                 @RequestParam(required = false) Optional<List<String>> genre,
                                 @RequestParam(required = false) Optional<String> gte,
                                 @RequestParam(required = false) Optional<String> director,
                                 @RequestParam(required = false, name = "agg") Optional<String> aggField,
                                 @RequestParam(required = false) Optional<Integer> from,
                                 @RequestParam(required = false) Optional<Integer> size
    ) throws ElasticsearchConnectionException, IndexNotFoundException {

        SearchRequest req = SearchRequest.of(indexRequest -> {

            indexRequest.index("films");

            if (from.isPresent())
                indexRequest.from(from.get());
            if (size.isPresent())
                indexRequest.size(size.get());

            var nestedQuery = QueryBuilders.bool();

            constructBoolQuery(nestedQuery, q);

            putFilters(type, genre, director, gte, nestedQuery);

            var functionQuery = wrapQueryWithRelevanceFunctionQuery(nestedQuery);

            //Assigning query to films index and placing it into request
            var wholeReq = indexRequest.index("films").query(new Query(functionQuery.build()));

            if (aggField.isPresent()) {
                wholeReq.aggregations(
                        aggField.get() + "_agg",
                        AggregationBuilders.terms().field(aggField.get()).size(1000).build()._toAggregation()
                );
            }

            return wholeReq;
        });

        try {
            var response = ClientCustomConfiguration.getClient().search(req, JsonData.class);
            if(response.hits().hits().isEmpty() && !q.equals("")) {
                return HiClientOps.run(q);
            } else {
                return ResultParser.getResultsAsString(aggField, response);
            }

        } catch (IOException e) {
            throw new ElasticsearchConnectionException(e);
        } catch (ElasticsearchException i) {
            throw new IndexNotFoundException("films", i);
        }


    }


    private void constructBoolQuery(BoolQuery.Builder builder, String q) {
        builder.must(mustClause -> {
            if(q.equals("")) {
                return mustClause.matchAll(matchAllQuery -> matchAllQuery);
            }
            return mustClause.multiMatch(multiMatchQuery -> multiMatchQuery
                    .fields("primaryTitle^20",
                            "primaryTitle.raw^50",
                            "originalTitle^40",
                            "originalTitle.raw^60")
                    .type(TextQueryType.BestFields)
                    .operator(Operator.Or)
                    .query(q)
                    .tieBreaker(0.3)
            );
        }
        ).should(shouldClause -> shouldClause
                .match(matchQuery -> matchQuery
                        .field("startYear")
                        .query(q)
                        .boost(60F)
                )
        ).should(secondShouldClause -> secondShouldClause
                .match(matchQuery -> matchQuery
                        .field("titleType")
                        .query("movie")
                        .boost(100F)
                )
        );
    }

    private void putFilters(Optional<List<String>> type,
                            Optional<List<String>> genre,
                            Optional<String> director,
                            Optional<String> gte,
                            BoolQuery.Builder query) {
        type.ifPresent(strings -> putFilter(strings, "titleType", query));
        genre.ifPresent(strings -> putFilter(strings, "genres", query));
        putNestedFilter(director, "directors.nconst", query);
        gte.ifPresent(s -> query.filter(filter -> filter.range(rangeFilter ->
                rangeFilter.field("averageRating").gte(JsonData.of(s)))));
    }

    private void putNestedFilter(Optional<String> value, String field, BoolQuery.Builder query) {
        value.ifPresent(presentValue -> query.filter(filterBuilder -> filterBuilder
                .nested(nestedFilter -> nestedFilter
                        .path(field.split("\\.")[0])
                        .query(queryBuilder -> queryBuilder
                                .term(termQuery -> termQuery.field(field).value(presentValue))
                        )
                )
        )
        );

    }

    private FunctionScoreQuery.Builder wrapQueryWithRelevanceFunctionQuery(BoolQuery.Builder query) {
        var functionQuery = QueryBuilders.functionScore();
        functionQuery.query(new Query(query.build())).functions(
                FunctionScore.of(functionOne ->
                        functionOne.fieldValueFactor(fValFactor -> fValFactor.field("averageRating")
                                .modifier(FieldValueFactorModifier.Ln2p))),

                FunctionScore.of(functionTwo ->
                        functionTwo.fieldValueFactor(fValFactor -> fValFactor.field("numVotes")
                                .factor(0.0001))
                )).scoreMode(FunctionScoreMode.Multiply).boostMode(FunctionBoostMode.Multiply);

        return functionQuery;
    }

    @GetMapping("id_search")
    public String getIndividualFilm(@RequestParam String id) throws ElasticsearchConnectionException, IndexNotFoundException {
        SearchRequest s = SearchRequest.of(request -> request
                .index("films")
                .query(query -> query.match(matchQuery -> matchQuery.field("_id").query(id)))
        );

        try {
            var response = ClientCustomConfiguration.getClient().search(s, JsonData.class);
            return ResultParser.getResultsAsString(Optional.ofNullable(null), response);
        } catch (IOException e) {
            throw new ElasticsearchConnectionException(e);
        } catch (ElasticsearchException i) {
            throw new IndexNotFoundException("films", i);
        }
    }

    private void putFilter(List<String> values, String field, BoolQuery.Builder builder) {
        builder.filter(queryField -> queryField.terms(termsFilter -> termsFilter
                        .field(field).terms(filter -> filter
                                .value(values.stream().map(FieldValue::of).toList())
                        )
                ) //Parseo de los typeFilters a FieldValue
        );
    }

    private List<Map<String, Object>> launchQuery(Query q, String index) {

        SearchRequest searchRequest = new SearchRequest.Builder().query(q).index(index).build();

        try {
            SearchResponse<JsonData> res = ClientCustomConfiguration.getClient().search(searchRequest, JsonData.class);
            return ResultParser.getResults(res);
        } catch (IOException e) {
            throw new ElasticsearchConnectionException(e);
        } catch (ElasticsearchException i) {
            throw new IndexNotFoundException(index, i);
        }
    }

}
