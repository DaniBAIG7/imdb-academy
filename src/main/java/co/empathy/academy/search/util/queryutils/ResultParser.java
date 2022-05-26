package co.empathy.academy.search.util.queryutils;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ResultParser {

    private static JsonArrayBuilder parseHits(SearchResponse<JsonData> response) {
        var hitsResult = Json.createArrayBuilder();
        response.hits().hits().stream()
                .filter(x -> x.source() != null)
                .map(x -> Json.createObjectBuilder()
                        .add("id", x.id())
                        .add("source", x.source().toJson())
                        .add("score", x.score())
                        .build()).toList().forEach(hitsResult::add);

        return hitsResult;
    }

    private static JsonArrayBuilder parseAggregations(String aggField, SearchResponse<JsonData> response) {
        String aggName = aggField + "_agg";
        var list = response.aggregations().get(aggName).sterms().buckets().array();

        var buckets = list.stream()
                .map(x -> Json.createObjectBuilder().add("key", x.key()).add("doc_count", x.docCount()).build());

        var result = Json.createArrayBuilder();

        buckets.forEach(result::add);

        return result;
    }

    private static String parseSuggestions(String suggField, SearchResponse<JsonData> response) {
        String suggName = suggField;
        var list = response.suggest().get(suggName).stream().toList();
        var result = Json.createArrayBuilder();

        list.forEach(suggResult -> result.add(suggResult._get().toString()));

        return result.build().toString();
    }

    /**
     * Private method that manages response for the term, terms and multimatch queries
     *
     * @param response
     * @return a json with the response
     */
    public static List<Map<String, Object>> getResults(SearchResponse<JsonData> response) {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = new ArrayList<>();

        for (Hit<JsonData> h : response.hits().hits()) {
            Map<String, Object> m = null;
            try {
                m = mapper.readValue(h.source().toString(), Map.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            result.add(m);
        }

        return result;
    }

    public static String getResultsAsString(Optional<String> aggFieldOpt,
                                     SearchResponse<JsonData> response) {
        var builder = Json.createObjectBuilder();
        builder.add("hits", parseHits(response));
        aggFieldOpt.ifPresent(agg -> builder.add("aggs", parseAggregations(agg, response)));

        return builder.build().toString();

    }
}
