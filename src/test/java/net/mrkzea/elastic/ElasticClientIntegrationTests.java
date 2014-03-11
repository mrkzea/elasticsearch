package net.mrkzea.elastic;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;


/**
 * Information about integration testing:
 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/integration-tests.html
 */
public class ElasticClientIntegrationTests extends ElasticsearchIntegrationTest {


    ElasticClient client;


    @Test
    public void shouldIndex() {
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticClient(testClient);
        client.index("test");
    }

}
