package co.empathy.academy.search.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ClientCustomConfiguration {

    // Create the low-level client
    private static RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200), new HttpHost("elasticsearch", 9200)).build();

    // Create the transport with a Jackson mapper
    private static ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

    // And create the API client
    private static ElasticsearchClient client = new ElasticsearchClient(transport);

    public static ElasticsearchClient getClient() {
        return client;
    }

    public static ElasticsearchTransport getTransport() {
        return transport;
    }

    public static RestClient getRestClient() {
        return restClient;
    }

}
