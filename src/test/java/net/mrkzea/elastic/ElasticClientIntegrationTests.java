package net.mrkzea.elastic;

import junit.framework.Assert;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;


/**
 * Information about integration testing:
 * http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/integration-tests.html
 */
public class ElasticClientIntegrationTests extends ElasticsearchIntegrationTest {


    ElasticMap client;


    @Test
    public void testPut() {
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        client.put("1", "test");
    }


    @Test
    public void testGet(){
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        client.put("1", "test");
        String result = client.get("1");
        System.out.println(result);
        Assert.assertEquals("{\"value\":\"test\"}",result);

    }



}
