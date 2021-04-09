package cn.su.es.score.first;

import java.net.InetAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTimeZone;

import cn.hutool.json.JSONUtil;

/**
 * @author SuZuQi
 * @title: EmployeeAggrApp
 * @projectName es_demo
 * @description: TODO
 * @date 2021/4/9
 */
public class EmployeeAggrApp {

    private static Log log = LogFactoryImpl.getLog(EmployeeAggrApp.class);

    /**
     * SearchResponse sr = node.client().prepareSearch() .addAggregation(
     * AggregationBuilders.terms("by_country").field("country")
     * .subAggregation(AggregationBuilders.dateHistogram("by_year") .field("dateOfBirth")
     * .dateHistogramInterval(DateHistogramInterval.YEAR)
     * .subAggregation(AggregationBuilders.avg("avg_children").field("children")) ) ) .execute().actionGet();
     * 
     * @param args
     */

    public static void main(String[] args) throws Exception {

        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        TransportClient client = new PreBuiltTransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        aggregate(client);
        client.close();
    }

    /**
     *
     * GET /company/employee/_search { "size": 0, "aggs": { "by_country": { "terms": { "field": "country" }, "aggs": {
     * "by_year": { "date_histogram": { "field": "join_date", "interval": "year", "format": "yyyy-MM-dd",
     * 
     * "order": {"avg_salary": "asc"} }, "aggs": { "avg_salary": { "avg": { "field": "salary" } } } } } } } }
     */

    // 首先按照国家分组
    // 然后按照入职年限分组
    // 统计每组的人员的平局工资
    private static void aggregate(TransportClient client) {
        // DateTimeZone timeZone = DateTimeZone.getDefault();
        DateTimeZone gmt = DateTimeZone.forID("GMT");
        SearchResponse searchResponse = client.prepareSearch("company").setTypes("employee").setSize(0)
            .addAggregation(AggregationBuilders.terms("by_country").field("country")
                .subAggregation(AggregationBuilders.dateHistogram("by_date").field("join_date")
                    .dateHistogramInterval(DateHistogramInterval.YEAR).timeZone(gmt).format("yyyy-MM-dd")
                    .order(Histogram.Order.aggregation("avg_by_salary", true))
                    .subAggregation(AggregationBuilders.avg("avg_by_salary").field("salary"))))
            .get();
        Aggregations aggregations = searchResponse.getAggregations();
        System.out.println(JSONUtil.toJsonStr(aggregations));
        List<Aggregation> aggregations1 = aggregations.asList();
        for (Aggregation aggregation : aggregations1) {
            List<Terms.Bucket> buckets = ((StringTerms)aggregation).getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String country = bucket.getKeyAsString();
                Aggregations aggregations2 = bucket.getAggregations();

                List<Aggregation> byDate = aggregations2.asList();
                for (Aggregation aggregation1 : byDate) {
                    List<Histogram.Bucket> dateBuckets = ((InternalDateHistogram)aggregation1).getBuckets();
                    for (Histogram.Bucket dateBucket : dateBuckets) {
                        String date = dateBucket.getKeyAsString();
                        Aggregations aggregations3 = dateBucket.getAggregations();
                        List<Aggregation> aggregations4 = aggregations3.asList();
                        for (Aggregation aggregation2 : aggregations4) {
                            Double value = ((InternalAvg)aggregation2).getValue();
                            String message = String.format("country是：%s,date是：%s,薪金平均值是:%f", country, date, value);
                            log.info(message);
                        }
                    }

                }

            }

        }
        System.out.println(aggregations1);

    }

}
