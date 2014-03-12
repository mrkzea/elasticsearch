package net.mrkzea.elastic;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;


/**
 * Simple implementation of a map based on elastic search
 */
public class ElasticMap {


    Client client;

    public ElasticMap(Client client) {
        this.client = client;
    }


    public void put(String key, String value) {
        try {

            client.prepareIndex("elastic_index", "elastic_type", key)
                    .setSource(jsonBuilder()
                            .startObject()

                            .field("value", value)
                            .endObject())
                    .execute()
                    .actionGet();

            // Without a refresh, search API won't return any hits
            // http://stackoverflow.com/questions/12692758/elasticsearch-java-api-matchall-search-query-doesnt-return-results?rq=1
            client.admin().indices().prepareRefresh().execute().actionGet();

        } catch (IOException e) {
            throw new RuntimeException("Unable to Index", e);
        }
    }


    public String get(String key) {
        GetResponse response = client.prepareGet("elastic_index", "elastic_type", key)
                .execute()
                .actionGet();
        String sourceAsString = response.getSourceAsString();
        return sourceAsString;
    }



    public void delete(String key) {
        client.prepareDelete("elastic_index", "elastic_type", key)
                .execute()
                .actionGet();
    }



    public void putAll(Entry[] entries) {
//        client.admin().indices().prepareCreate("elastic_index").execute().actionGet();

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (Entry e : entries) {
            try {
                bulkRequest.add(client.prepareIndex("elastic_index", "elastic_type", e.key)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("ID", e.key)
                                .field("value", e.value).endObject()));

            } catch (IOException e1) {
                throw new RuntimeException("Bulk failed", e1);
            }
        }

        BulkResponse bulkResponse  = bulkRequest.execute().actionGet();

        if (bulkResponse .hasFailures()) {
            // process failures by iterating through each bulk response item
        }
        //Without a refresh, search API won't return any hits
        // http://stackoverflow.com/questions/12692758/elasticsearch-java-api-matchall-search-query-doesnt-return-results?rq=1
        client.admin().indices().prepareRefresh().execute().actionGet();

    }


    public String search(String term) throws Exception{
        SearchResponse searchResponse = client.prepareSearch("elastic_index")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(matchQuery("value", term))
                .setFrom(0).setSize(60).setExplain(true)
                .setTypes("elastic_type")
                .execute()
                .actionGet();

//
//        final QueryBuilder queryBuilder = queryString("q=value:big");
//        SearchResponse searchResponse = client.prepareSearch("elastic_index")
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .addField("value")
//                .setFrom(0).setSize(1000)
//                .setQuery(queryBuilder)
//                .setTypes("elastic_type")
//                .setTimeout(TimeValue.timeValueSeconds(10))
//                .execute().actionGet();

//        SearchResponse searchResponse = client.prepareSearch("elastic_index")
//                .setSearchType(SearchType.SCAN)
//                .setScroll(new TimeValue(600000))
//                .setTypes("elastic_type")
//                .setQuery(QueryBuilders.matchAllQuery())
//                .setFrom(0).setSize(100).setExplain(true)
//                .execute().actionGet();


        SearchHits hits = searchResponse.getHits();
        SearchHit firstHit = hits.getAt(0);

        return firstHit.getSourceAsString();

    }
}
