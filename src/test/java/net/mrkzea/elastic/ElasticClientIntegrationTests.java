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




    @Test
    public void testDelete(){
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        client.put("1", "test");
        String result = client.get("1");
        Assert.assertEquals("{\"value\":\"test\"}", result);
        client.delete("1");
        Assert.assertNull(client.get("1"));
    }



    @Test
    public void testBulk(){
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        Entry e1 = new Entry("1", "small");
        Entry e2 = new Entry("2", "big");
        Entry[] entries = new Entry[]{e1, e2};
        client.putAll(entries);
        Assert.assertEquals("{\"ID\":\"2\",\"value\":\"big\"}", client.get("2"));
    }



    @Test
    public void testSearch() throws  Exception{
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        Entry e1 = new Entry("1", "small");
        Entry e2 = new Entry("2", "big");
        Entry[] entries = new Entry[]{e1, e2};
        client.putAll(entries);

        String term = "big";
        String result = client.search(term);
        Assert.assertEquals("{\"ID\":\"2\",\"value\":\"big\"}", result);
    }


    @Test
    public void testSubstringSearch() throws  Exception{
        Client testClient = ElasticsearchIntegrationTest.client();
        client = new ElasticMap(testClient);
        Entry e1 = new Entry("1", "small");
        Entry e2 = new Entry("2", "http://company.com?id=1022&user=claire");
        Entry[] entries = new Entry[]{e1, e2};
        client.putAll(entries);

        String term = "id=1022";
        String result = client.search(term);
        Assert.assertEquals("{\"ID\":\"2\",\"value\":\"http://company.com?id=1022&user=claire\"}", result);
    }
}
