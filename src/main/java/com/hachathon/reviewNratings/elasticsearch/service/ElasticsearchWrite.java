package com.hachathon.reviewNratings.elasticsearch.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hachathon.reviewNratings.ReviewNratingsApplication;
import com.hachathon.reviewNratings.document.ReviewsAndRatings;
import lombok.SneakyThrows;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hachathon.reviewNratings.constants.Constant.INDEX;
import static com.hachathon.reviewNratings.constants.Constant.TYPE;

@Service
public class ElasticsearchWrite {

    @Autowired
    public RestHighLevelClient client;

    public void createIndex(){
        CreateIndexRequest request = new CreateIndexRequest("twitter");
    }

    @SneakyThrows
    public String writeDoc(List<ReviewsAndRatings> documents){
        Map r1 = null;

        for(ReviewsAndRatings doc:documents){
            if(null != doc){
                UUID uuid = UUID.randomUUID();
                ObjectMapper objectMapper = new ObjectMapper();
                Map documentMapper = objectMapper.convertValue(doc, Map.class);
                IndexRequest indexRequest = new IndexRequest(INDEX)
                        .source(documentMapper);
                indexRequest.id(uuid.toString());
                r1 = documentMapper;
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            }

        }

        /*UUID uuid = UUID.randomUUID();
        document.setId(uuid.toString());
        ObjectMapper objectMapper = new ObjectMapper();
        Map documentMapper = objectMapper.convertValue(document, Map.class);
        IndexRequest indexRequest = new IndexRequest(INDEX)
                .source(documentMapper);
        indexRequest.id(uuid.toString());
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return indexResponse
                .getResult()
                .name();*/

        return "success";
    }

    @SneakyThrows
    public BulkResponse writeBulkDocs(List<ReviewsAndRatings> docs){
        BulkRequest request = new BulkRequest();
        int count = 0;
        docs.forEach(doc -> {
            if(null != doc){
                ObjectMapper objectMapper = new ObjectMapper();
                UUID uuid = UUID.randomUUID();
                Map documentMapper = objectMapper.convertValue(doc, Map.class);
                IndexRequest indexRequest = new IndexRequest(INDEX)
                        .source(documentMapper);
                indexRequest.id(uuid.toString());

                request.add(indexRequest);
            }else{
                System.out.println(count+"  of records did not publish.   " +doc.getEntityId());
            }

        });
        System.out.println("inside the loop");
        try{
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            return bulkResponse;
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        //BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        return null;
    }
}
