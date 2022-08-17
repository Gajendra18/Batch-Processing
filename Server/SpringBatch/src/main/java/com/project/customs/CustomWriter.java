package com.project.customs;

import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

@Slf4j
public class CustomWriter implements ItemWriter<Document> {

    @Autowired
    private MongoTemplate mongoTemplate;

    private String collectionName;

    public CustomWriter() {
    }

    public CustomWriter(String collectionName) {
        this.collectionName = collectionName;
    }

    @Override
    public void write(List<? extends Document> items) throws Exception {
        MongoCollection<Document> collection;
        if (!mongoTemplate.getCollectionNames().contains(this.collectionName)) {
            collection = mongoTemplate.createCollection(this.collectionName);
        } else {
            collection = mongoTemplate.getCollection(this.collectionName);
        }
        collection.insertMany(items);
    }


}
