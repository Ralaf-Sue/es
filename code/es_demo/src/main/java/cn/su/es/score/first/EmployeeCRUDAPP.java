package cn.su.es.score.first;

import java.net.InetAddress;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * @author SuZuQi
 * @title: EmployeeCRUDAPP
 * @projectName es_demo
 * @description: TODO
 * @date 2021/4/8
 */
public class EmployeeCRUDAPP {

    public static void main(String[] args) throws Exception {
        // 先构建client
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        TransportClient client = new PreBuiltTransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        // .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost1"),9300));
        // 创建document
        // createEmployee(client);
        // updateEmployee(client);
        deleteEmployee(client);
        getEmployee(client);
        client.close();
    }

    /**
     * 创建员工信息（创建一个document）
     * 
     * @param client
     */
    private static void createEmployee(TransportClient client) throws Exception {
        IndexResponse indexResponse = client.prepareIndex("company", "employee", "1")
            .setSource(XContentFactory.jsonBuilder().startObject().field("name", "jack").field("age", 27)
                .field("position", "technique").field("country", "china").field("join_date", "2017-01-01")
                .field("salary", 10000).endObject()

            ).get();
        System.out.println(indexResponse.getResult());
    }

    private static void getEmployee(TransportClient client) {
        GetResponse getResponse = client.prepareGet("company", "employee", "1").get();
        System.out.println(getResponse.getSourceAsString());
    }

    private static void updateEmployee(TransportClient client) throws Exception {
        UpdateResponse updateResponse = client.prepareUpdate("company", "employee", "1")
            .setDoc(XContentFactory.jsonBuilder().startObject().field("salary", 15000).endObject()).get();
        System.out.println(updateResponse.getResult());
    }

    private static void deleteEmployee(TransportClient client) {
        DeleteResponse deleteResponse = client.prepareDelete("company", "employee", "1").get();
        System.out.println(deleteResponse.getResult());

    }

}
