package www.codeedge.www.codeedge.estest;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import www.codeedge.pojo.Journalism;

import java.net.InetAddress;
import java.util.Map;

public class EsTest {
    @Test
    public void createIndex() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));
        client.admin().indices()
                .prepareCreate("test02")
                .get();

        client.close();
    }
    @Test
    public void addMappings() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("journalism")
                        .startObject("properties")
                            .startObject("id")
                                .field("type","long")
                                .field("store",true)
                                .field("index",false)
                            .endObject()
                            .startObject("title")
                                .field("type","text")
                                .field("store",true)
                                .field("index",true)
                                .field("analyzer","ik_smart")
                            .endObject()
                            .startObject("content")
                                .field("type","text")
                                .field("store",true)
                                .field("analyzer","ik_max_word")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        client.admin().indices()
                .preparePutMapping("test02")
                .setType("journalism")
                .setSource(builder)
                .get();

        client.close();

    }
    @Test
    public void createMappings() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("journalism")
                .startObject("properties")
                .startObject("id")
                .field("type","long")
                .field("store",true)
                .field("index",false)
                .endObject()
                .startObject("title")
                .field("type","text")
                .field("store",true)
                .field("index",true)
                .field("analyzer","ik_smart")
                .endObject()
                .startObject("content")
                .field("type","text")
                .field("store",true)
                .field("analyzer","ik_max_word")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        client.admin().indices()
                .prepareCreate("test03")
                .setSource(builder)
                .get();

        client.close();
    }

    @Test
    public void delIndex() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        client.admin().indices()
                .prepareDelete("test02")
                .get();

        client.close();

    }

    @Test
    public void addDocument() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        Journalism journalism = new Journalism();
        journalism.setId(4l);
        journalism.setTitle("2020年产业扶贫工作指导意见  辟谣  求助 标题4");
        journalism.setContent("坚决打好疫情防控的人民战争 不获全胜决不轻言成功");

        ObjectMapper objectMapper = new ObjectMapper();
        String dom = objectMapper.writeValueAsString(journalism);
        System.out.println(dom);
        client.prepareIndex()
                .setIndex("test02")
                .setType("journalism")
                .setId("4")
                .setSource(dom, XContentType.JSON)
                .get();

        client.close();
    }

    @Test
    public void delDocument() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        client.prepareDelete("test02","journalism","AXB2kOaizo2Gdf5tuM1k")
                .get();

        client.close();
    }

    @Test
    public void updDocument()throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        Journalism journalism = new Journalism();
        journalism.setId(1l);
        journalism.setTitle("我是修改后的标题");
        journalism.setContent("我是修改后的内容");

        ObjectMapper objectMapper = new ObjectMapper();

        String upddom = objectMapper.writeValueAsString(journalism);

        client.prepareUpdate("test02","journalism","2")
                .setDoc(upddom,XContentType.JSON)
                .get();
        client.close();

    }

    @Test
    public void selById() throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        QueryBuilder queryBuilder = QueryBuilders.idsQuery()
                .addIds("2")
                .types("journalism");

        SearchResponse searchResponse = client.prepareSearch("test02")
                .setQuery(queryBuilder)
                .get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("总记录数："+hits.totalHits);

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit s:searchHits) {
            Map<String, Object> source = s.getSource();
            System.out.println(source);

        }

        client.close();
    }

    @Test
    public void selByTerm()throws Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        QueryBuilder termQueryBuilder = QueryBuilders.termQuery("title","标题");

        SearchResponse searchResponse = client.prepareSearch("test02")
                .setTypes("journalism")
                .setQuery(termQueryBuilder)
                .get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("命中："+hits.totalHits);

        SearchHit[] hits1 = hits.getHits();
        for (SearchHit s:hits1) {
            System.out.println("-------------");
            Map<String, Object> source = s.getSource();
            System.out.println(source);
        }

        client.close();

    }

    @Test
    public void selByString() throws  Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("我是修改后")
                .defaultField("title");

        SearchResponse searchResponse = client.prepareSearch("test02")
                .setTypes("journalism")
                .setQuery(queryBuilder)
                .get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("命中："+hits.totalHits);

        client.close();

    }

    @Test
    public void end() throws  Exception{
        Settings settings = Settings.builder()
                .put("cluster.name","my-elasticsearch")
                .build();
        
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9301));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9302));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9304));

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "标题");

        HighlightBuilder highlightBuilder = new HighlightBuilder()
                .field("title")
                .preTags("<h1>")
                .postTags("<h1/>");


        SearchResponse searchResponse = client.prepareSearch("test02")
                .setTypes("journalism")
                .setQuery(termQueryBuilder)
                .highlighter(highlightBuilder)
                .setFrom(0)
                .setSize(3)
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println(hits.totalHits);
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit s:hits1) {
            Map<String, Object> source = s.getSource();
            System.out.println(source);
            Map<String, HighlightField> highlightFields = s.getHighlightFields();
            System.out.println(highlightFields);
            HighlightField title = highlightFields.get("title");
            System.out.println(title);
            Text[] fragments = title.getFragments();
            System.out.println(fragments[0]);

        }

        client.close();
    }

}
