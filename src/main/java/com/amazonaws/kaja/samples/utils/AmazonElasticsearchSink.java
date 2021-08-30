package com.amazonaws.kaja.samples.utils;

import com.amazonaws.kaja.samples.ClickstreamProcessor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class AmazonElasticsearchSink {

    private static final Logger LOG = LoggerFactory.getLogger(ClickstreamProcessor.class);
    private static final String ES_SERVICE_NAME = "es";
    private static final int FLUSH_MAX_ACTIONS = 10_000;
    private static final long FLUSH_INTERVAL_MILLIS = 1_000;
    private static final int FLUSH_MAX_SIZE_MB = 1;


    public static <T> ElasticsearchSink<T> buildElasticsearchSink(String elasticsearchEndpoint, String region, String indexName, String type) {
        final List<HttpHost> httpHosts = Arrays.asList(HttpHost.create(elasticsearchEndpoint));


        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<T>() {
                    public IndexRequest createIndexRequest(T element) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("data", ClickstreamProcessor.toJson(element));

                        return Requests.indexRequest()
                                .index(indexName)
                                .type(type)
                                .source(ClickstreamProcessor.toJson(element), XContentType.JSON);
                    }

                    @Override
                    public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(FLUSH_MAX_ACTIONS);
        esSinkBuilder.setBulkFlushInterval(FLUSH_INTERVAL_MILLIS);
        esSinkBuilder.setBulkFlushMaxSizeMb(FLUSH_MAX_SIZE_MB);
        esSinkBuilder.setBulkFlushBackoff(true);

        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );


        return esSinkBuilder.build();


    }
}
