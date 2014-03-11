package net.mrkzea.elastic;


import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class ElasticClient  {


    Client client;

    public ElasticClient(Client client){
        this.client = client;
    }


    public void index(String text) {

        try {
            final XContentBuilder jsonBuilder = jsonBuilder()
                    .startObject()
                        .field("text", text)
                    .endObject();
        } catch (IOException e) {
            throw new RuntimeException("Unable to Index",e);

        }

    }

}
