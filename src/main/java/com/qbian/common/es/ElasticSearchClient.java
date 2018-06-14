package com.qbian.common.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.xContent;

/**
 * Created by Qbian on 2017/5/11.
 */
@Component
public class ElasticSearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchClient.class);

    /**
     * 文档类型名称 =》 数据库名
     */
    public static final String INDEX_TYPE = "test";

    /**
     * client
     */
    private static TransportClient client;

    /**
     * 集群节点 ip 数组
     */
    @Value("${es.ips}")
    private String ipStr;

    /**
     * 集群内节点通讯端口
     */
//    @Value("${es.port}")
//    private Integer port;

    /**
     * 集群名称
     */
    @Value("${es.cluster}")
    private String clusterName;

    /**
     * 初始化
     */
    @PostConstruct
    public void init() {
        try {
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();

            client = new PreBuiltTransportClient(settings);
            if(!StringUtils.isEmpty(ipStr)) {
                String[] ips = ipStr.split(",");
                for(String ip : ips) {
                    if(!StringUtils.isEmpty(ip)) {
                        String ipstr=ip.split(":")[0];
                        Integer po=Integer.parseInt(ip.split(":")[1]);
                        LOG.debug("ElasticSearchClient init link .", ipstr+po);
                        client.addTransportAddress(new TransportAddress(InetAddress.getByName(ipstr),po) );
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("ElasticSearchClient init error .", e);
        }
    }

    /**
     * 销毁
     */
    @PreDestroy
    public void close() {
        try {
            if(client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOG.error("ElasticSearchClient close error .", e);
        }
    }

    public void  createMapping(String type,XContentBuilder mappingBuilder){
         try {
             String settingsSource="{\n" +
                     "\t   \"analysis\": {\n" +
                     "\t      \"analyzer\" : {\n" +
                     "                \"pinyin_analyzer\" : {\n" +
                     "                    \"tokenizer\" : \"my_pinyin\"\n" +
                     "                    }\n" +
                     "            },\n" +
                     "            \"tokenizer\" : {\n" +
                     "                \"my_pinyin\" : {\n" +
                     "                    \"type\" : \"pinyin\",\n" +
                     "                    \"keep_separate_first_letter\" : false,\n" +
                     "                    \"keep_full_pinyin\" : true,\n" +
                     "                    \"keep_original\" : true,\n" +
                     "                    \"limit_first_letter_length\" : 16,\n" +
                     "                    \"lowercase\" : true,\n" +
                     "                    \"remove_duplicated_term\" : true\n" +
                     "                }\n" +
                     "            }\n" +
                     "\t   }\n" +
                     "\t\t}";
                    String mapString="{\n" +
                            "      \"properties\": {\n" +
                            "        \"name\": {\n" +
                            "          \"type\": \"text\",\n" +
                            "          \"fields\": {\n" +
                            "           \"item_title_ik\" : {\"type\" : \"text\", \"analyzer\" :\"ik_max_word\",\"search_analyzer\":\"ik_max_word\"},\n" +
                            "\t\t   \"item_title_pinyin\" : {\"type\" : \"text\", \"analyzer\" :\"pinyin_analyzer\",\"search_analyzer\":\"pinyin_analyzer\"}\n" +
                            "        }\n" +
                            "      },\n" +
                            "      \"interest\": {\n" +
                            "      \t\"type\": \"text\",\n" +
                            "      \t\"analyzer\": \"ik_max_word\",\n" +
                            "      \t\"search_analyzer\": \"ik_max_word\"\n" +
                            "      }\n" +
                            "    }\n" +
                            "  }";
             if(!isExistsIndex(INDEX_TYPE)){
                 client.admin().indices().prepareCreate(INDEX_TYPE).setSettings(settingsSource,XContentType.JSON).addMapping("person",mapString,XContentType.JSON).execute().actionGet();
//                 client.admin().indices().prepareCreate(INDEX_TYPE).execute().actionGet();
             }


//             //更改设置先关闭index
//             CloseIndexRequest requestclose=Requests.closeIndexRequest(INDEX_TYPE);
//             client.admin().indices().close(requestclose);
//
//             UpdateSettingsRequest updateSettingsRequest=Requests.updateSettingsRequest(INDEX_TYPE).settings(source,XContentType.JSON);
//             ActionFuture<UpdateSettingsResponse> re=client.admin().indices().updateSettings(updateSettingsRequest);
//             System.out.println(JSONObject.toJSONString(re));
//
//             OpenIndexRequest requestopen=Requests.openIndexRequest(INDEX_TYPE);
//             client.admin().indices().open(requestopen);

             PutMappingRequest mapping = Requests.putMappingRequest(INDEX_TYPE).type(type).source(mappingBuilder);
             client.admin().indices().putMapping(mapping).actionGet();
         }catch(Exception e){
              e.printStackTrace();
             LOG.error("error :getInfo",e.getMessage());
         }
    }


    /**
     * 创建索引，保存数据
     * @param type 文档类型
     * @param jsonData json 格式的数据
     */
    public void  createIndex(String type, JSONObject jsonData) {
        IndexRequestBuilder requestBuilder = client.prepareIndex(INDEX_TYPE, type);
        System.out.println(jsonData.toJSONString());
        String req=jsonData.toJSONString();
        requestBuilder.setSource(req, XContentType.JSON).execute().actionGet();
    }

    /**
     * 检索集群
     * @param queryBuilder 查询引擎
     * @param type 查询文档类型
     * @param pageNo 起始页
     * @param pageSize 当页数量
     * @return
     */
    public JSONArray search(QueryBuilder queryBuilder, String type, int pageNo, int pageSize) {
        // 检索集群数据
        SearchResponse searchResponse = client.prepareSearch(INDEX_TYPE).setTypes(type)
                .setQuery(queryBuilder).addSort("date", SortOrder.DESC)
                .setFrom(pageNo * pageSize).setSize(pageSize)
                .execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();

        // 封装检索结果
        JSONArray result = new JSONArray();
        if(hits != null && hits.length > 0) {
            for(SearchHit hit : hits) {
                JSONObject data = new JSONObject();
                Map<String, Object> m=  hit.getSourceAsMap();
                for (String s : m.keySet()) {
                    data.put(s, m.get(s));
                }
                result.add(data);
            }
        }
        return result;
    }

    /**
     * 检索匹配关键字高亮
     * @param queryBuilder 查询引擎
     * @param type 文档类型
     * @param highlightFieldList 高亮清单
     * @param pageNo 页码
     * @param pageSize 当页显示数据量
     * @return 查询结果
     */
    public JSONArray searchHighlight(QueryBuilder queryBuilder, String type, List<String> highlightFieldList
            , int pageNo, int pageSize) {
        StopWatch clock = new StopWatch();
        clock.start();

        // 设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder().requireFieldMatch(true);
        if(highlightFieldList != null) {
            for(String field : highlightFieldList) {
                highlightBuilder.field(field, 10240);
            }
        }
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");

        SearchResponse searchResponse = client.prepareSearch(INDEX_TYPE).setTypes(type)
                .setQuery(queryBuilder).highlighter(highlightBuilder)
                .addSort("date", SortOrder.DESC)
                .setFrom(pageNo * pageSize).setSize(pageSize)
                .setExplain(true).execute().actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();

        clock.stop();
        LOG.info("searchHighlight: {} ms", clock.getTotalTimeMillis());

        // 封装查询结果
        JSONArray result = new JSONArray();
        if(hits != null && hits.length > 0) {
            for(SearchHit hit : hits) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                JSONObject data = new JSONObject();
                Map<String, Object> m=hit.getSourceAsMap();
                for (String s : m.keySet()) {

                    // 保存高亮字段
                    if(highlightFields != null && highlightFields.containsKey(s)) {
                        HighlightField titleField = highlightFields.get(s);
                        Text[] fragments = titleField.fragments();
                        StringBuilder sb = new StringBuilder();
                        for(Text text : fragments) {
                            sb.append(text);
                        }
                        data.put(s, sb.toString());
                    } else {
                        data.put(s, m.get(s));
                    }
                }
                result.add(data);
            }
        }

        return result;
    }
    /**
     * 判断指定的索引名是否存在
     * @param indexName 索引名
     * @return  存在：true; 不存在：false;
     */
    public boolean isExistsIndex(String indexName){
        IndicesExistsResponse response =
                client.admin().indices().exists(
                        new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
        return response.isExists();
    }

}
