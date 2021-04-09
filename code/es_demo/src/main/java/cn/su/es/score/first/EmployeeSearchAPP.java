package cn.su.es.score.first;

import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author SuZuQi
 * @title: EmployeeSearchAPP
 * @projectName es_demo
 * @description: TODO
 * @date 2021/4/8
 */
public class EmployeeSearchAPP {

    public static void main(String[] args) throws Exception {

        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        TransportClient client = new PreBuiltTransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        prepareData(client);
        searchData(client);
        client.close();
    }

    private static void prepareData(TransportClient client) throws Exception {
        client.prepareIndex("company", "employee", "1")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "jack").field("age", 27)
                .field("postion", "technique software").field("country", "china").field("join_date", "2017-01-01")
                .field("salary", 10000).endObject())
            .get();

        client.prepareIndex("company", "employee", "2")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "marry").field("age", 35)
                .field("postion", "technique  manager").field("country", "china").field("join_date", "2017-01-01")
                .field("salary", 12000).endObject())
            .get();

        client.prepareIndex("company", "employee", "3")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "tom").field("age", 32)
                .field("postion", "senior technique software").field("country", "china")
                .field("join_date", "2016-01-01").field("salary", 11000).endObject())
            .get();

        client.prepareIndex("company", "employee", "4")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "jue").field("age", 25)
                .field("postion", "junior finance").field("country", "usa").field("join_date", "2016-01-01")
                .field("salary", 7000).endObject())
            .get();

        client.prepareIndex("company", "employee", "5")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "mike").field("age", 37)
                .field("postion", "finance manager").field("country", "usa").field("join_date", "2015-01-01")
                .field("salary", 15000).endObject())
            .get();
    }

    /**
     * 需求 搜索职位中包含technique的员工 同时要求age在30-40之间 分页查询第一页
     */
    /**
     * client.prepareSearch("index1", "index2").setTypes("type1", "type2").setQuery(QueryBuilders.termQuery("", ""))
     * .setPostFilter(QueryBuilders.rangeQuery("age").from(0).to(18)).setFrom(0).setSize(60).get();
     */

    private static void searchData(TransportClient client) {
        SearchResponse searchResponse = client.prepareSearch("company").setTypes("employee")
            .setQuery(QueryBuilders.termQuery("postion", "technique"))
            .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40)).setFrom(0).setSize(1).get();
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            System.out.println(hits[i].getSourceAsString());
        }
    }
}
