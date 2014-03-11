package net.mrkzea.elastic;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


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
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (Entry e : entries) {
            try {
                bulkRequest.add(client.prepareIndex("elastic_index", "elastic_type", e.key)
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("value", e.value).endObject()));

            } catch (IOException e1) {
                throw new RuntimeException("Bulk failed", e1);
            }
        }

        BulkResponse bulkResponse  = bulkRequest.execute().actionGet();

        if (bulkResponse .hasFailures()) {
            // process failures by iterating through each bulk response item
        }

    }
}
