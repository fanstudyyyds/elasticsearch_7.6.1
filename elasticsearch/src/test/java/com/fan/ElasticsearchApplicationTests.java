package com.fan;

import com.alibaba.fastjson.JSON;
import com.fan.pojo.Da;
import com.fan.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Test
    void contextLoads() throws Exception {
        //测试索引的创建
        //1创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("jd");
        //2执行请求
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //测试获得索引
    @Test
    void testExistCreateIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("fan");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    @Test
    void testDeleteIndex() throws IOException {
        //删除一个索引
        DeleteIndexRequest delete = new DeleteIndexRequest("fd");
        AcknowledgedResponse delete1 = client.indices().delete(delete, RequestOptions.DEFAULT);
        System.out.println(delete1);
    }

    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        User user = new User("范hh", 15);
        //创建请求
        IndexRequest request = new IndexRequest("jd");
        //规则
        request.id("2");
        request.timeout("1s");
        // request.timeout(TimeValue.timeValueSeconds(1));
        IndexRequest source = request.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse index = client.index(request, RequestOptions.DEFAULT);
        System.out.println(index.status());//返回状态
    }


    //判断文档是否存在
    @Test
    void testIsExistsDocument() throws IOException {
        GetRequest getRequest = new GetRequest("fan", "1");
        //不获取返回的_source的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);

    }


    //获得文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("fan", "2");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());//打印文档内容
        System.out.println(getResponse);//返回全部内容

    }

    //更新文档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("fan", "1");
        updateRequest.timeout("1s");
        User user = new User("咯", 14);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(update.status());

    }

    //删除文档信息
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("fan", "3");
        deleteRequest.timeout("1s");
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    //大批量存入文档信息
    @Test
    void testBulkDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> users = new ArrayList<>();
        users.add(new User("谷e歌", 3));
        users.add(new User("谷w歌2", 3));
        users.add(new User("谷w歌3", 3));
//批量增加修改删除修改 IndexRequest
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(new IndexRequest("fan")
                    //.id("" + i + 3)
                    .source(JSON.toJSONString(users.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());
    }


    //搜索
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("fan");
        //构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //查询条件 QueryBuilders工具类
        //QueryBuilders.termQuery() 精确查询
        //QueryBuilders.matchAllQuery() 匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "谷歌");
        // MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        for (SearchHit hit : search.getHits()) {
            System.out.println(hit.getSourceAsMap());
        }


    }


    @Test
    void testBulkDo2cument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<Da> users = new ArrayList<>();
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
        users.add(new Da("vue学习黄金组合套装 （京东套装共3册）","980.30","http://img13.360buyimg.com/n1/s200x200_jfs/t1/21072/33/12467/90043/5c9854b6E46278aca/fe5e2d55c0d908ee.jpg"));
//批量增加修改删除修改 IndexRequest
        for (int i = 0; i < users.size(); i++) {
            bulkRequest.add(new IndexRequest("jd")
                    //.id("" + i + 3)
                    .source(JSON.toJSONString(users.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
        System.out.println(bulk.hasFailures());
    }

}
