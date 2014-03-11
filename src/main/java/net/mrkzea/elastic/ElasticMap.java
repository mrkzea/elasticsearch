package net.mrkzea.elastic;


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

}
