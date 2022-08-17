package com.project.customs;

import org.bson.Document;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class CustomProcessor implements ItemProcessor<Document, Document> {

    @Override
    public Document process(Document item) throws Exception {
        return item;
    }

}
