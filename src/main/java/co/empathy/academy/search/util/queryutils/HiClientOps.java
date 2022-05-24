package co.empathy.academy.search.util.queryutils;

import co.empathy.academy.search.exception.InternalServerException;
import co.empathy.academy.search.util.HighClientCustomConfiguration;
import jakarta.json.Json;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;

import java.io.IOException;

public class HiClientOps {

    private static RestHighLevelClient client = HighClientCustomConfiguration.getClient();
    private HiClientOps() {}

    private static void termSuggestion(String field, String q, SuggestBuilder builder) {
        SuggestionBuilder<TermSuggestionBuilder> termSuggestionBuilder =
                SuggestBuilders.termSuggestion(field).text(q);
        builder.addSuggestion("spellcheck", termSuggestionBuilder);
    }

    private static void phraseSuggestion(String field, String q, SuggestBuilder builder) {
        SuggestionBuilder<PhraseSuggestionBuilder> phraseSuggestionBuilder =
                SuggestBuilders.phraseSuggestion(field)
                        .text(q)
                        .maxErrors(3)
                        .gramSize(3)
                        .analyzer("custom_fkinawesome_analyzer");
        builder.addSuggestion("phrase", phraseSuggestionBuilder);
    }

    public static String run(String q) {
        var searchRequest = new SearchRequest("films");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SuggestBuilder suggestBuilder = new SuggestBuilder();
        termSuggestion("primaryTitle", q, suggestBuilder);
        phraseSuggestion("primaryTitle", q, suggestBuilder);

        searchSourceBuilder.suggest(suggestBuilder);

        searchRequest.source(searchSourceBuilder);

        try {
            var suggestResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            TermSuggestion termSuggest = suggestResponse.getSuggest().getSuggestion("spellcheck");
            PhraseSuggestion phraseSuggest = suggestResponse.getSuggest().getSuggestion("phrase");

            var result = Json.createObjectBuilder();

            var termOptionArray = Json.createArrayBuilder();
            var phraseOptionArray = Json.createArrayBuilder();

            termSuggest.getEntries().get(0).getOptions()
                    .stream().map(option ->
                            Json.createObjectBuilder()
                                    .add("score", option.getScore())
                                    .add("freq", option.getFreq())
                                    .add("text", option.getText().toString())
                                    .build())
                    .forEach(termOptionArray::add);

            phraseSuggest.getEntries().get(0).getOptions()
                    .stream().map(option ->
                            Json.createObjectBuilder()
                                    .add("score", option.getScore())
                                    .add("text", option.getText().toString())
                                    .build())
                    .forEach(phraseOptionArray::add);

            result.add("hits", Json.createArrayBuilder().build());
            result.add("aggs", Json.createArrayBuilder().build());
            result.add("term-suggestions", termOptionArray.build());
            result.add("phrase-suggestions", phraseOptionArray.build());

            return result.build().toString();

        } catch (IOException e) {
            throw new InternalServerException(e);
        }
    }

}
