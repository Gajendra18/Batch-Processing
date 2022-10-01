package com.project.customs;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.project.jobConfiguration.SimpleJobLauncher;
import com.project.utility.BatchPartitionHelper;
import com.project.utility.DataTypeParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.Document;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class CustomFileReader implements ItemStreamReader<Document> {

    private int count;

    private List<Document> users;

    private static final String CURRENT_INDEX = "current.index";

    @Value("${application.bucket.name}")
    private String bucketName;


    @Autowired
    private AmazonS3 s3Client;

    @Autowired
    private DataTypeParser dataTypeParser;

    @Autowired
    BatchPartitionHelper batchPartitionHelper;

    private final String fileName;

    private final Map<String, Integer> index;



    public CustomFileReader(String fileName, Map<String, Integer> index) {
        this.fileName = fileName;
        this.index = index;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey(CURRENT_INDEX)) {
            this.count = Integer.parseInt(Long.toString(executionContext.getLong(CURRENT_INDEX)));
        } else {
            this.count = 0;
        }

    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.put(CURRENT_INDEX, (long) this.count);
    }

    @Override
    public Document read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        Document user = null;
        if (this.count < this.users.size()) {
            user = this.users.get(this.count);
            this.count++;
        } else {
            this.count = 0;
        }
        return user;
    }


    @Override
    public void close() throws ItemStreamException {
        // Default Implementation
    }


    @PostConstruct
    public void mapInput() throws Exception {
        this.users = new ArrayList<>();
        this.count = 0;
        int startIndex = this.index.get("startingIndex");
        int endIndex = this.index.get("endingIndex");
        try {
            S3Object object = s3Client.getObject(bucketName, this.fileName);
            try (S3ObjectInputStream inputStream = object.getObjectContent();
                 InputStreamReader isr = new InputStreamReader(inputStream);
                 BufferedReader br = new BufferedReader(isr)) {

                if (batchPartitionHelper.getFileType(this.fileName).equals("CSV")) {
                    String line = null;
                    String[] headers = br.readLine().split(",");
                    int lineCount = 2;
                    while (lineCount < startIndex) {
                        br.readLine();
                        lineCount++;
                    }
                    while (lineCount <= endIndex && (line = br.readLine()) != null) {
                        String[] array = line.split(",");
                        Document doc = new Document();
                        int i = 0;
                        for (String fieldName : headers) {
                            doc.put(fieldName, dataTypeParser.stringToDataType(array[i]));
                            i++;
                        }
                        lineCount++;
                        this.users.add(doc);
                    }
                } else {
                    XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
                    XSSFSheet sheet = workbook.getSheetAt(0);
                    DataFormatter dataFormatter = new DataFormatter();
                    Iterator<Row> rowIterator = sheet.iterator();
                    List<String> header = new ArrayList<>();
                    Row headers = rowIterator.next();
                    Iterator<Cell> headerIterator = headers.cellIterator();
                    while (headerIterator.hasNext()) {
                        Cell cell = headerIterator.next();
                        header.add(dataFormatter.formatCellValue(cell));
                    }

                    int lineCount = 2;
                    while (lineCount < startIndex) {
                        rowIterator.next();
                        lineCount++;
                    }

                    while (lineCount <= endIndex && rowIterator.hasNext()) {
                        Row row = rowIterator.next();
                        Iterator<Cell> cellIterator = row.cellIterator();
                        Document doc = new Document();
                        for (String fieldName : header) {
                            Cell cell = cellIterator.next();
                            String value = dataFormatter.formatCellValue(cell);
                            doc.put(fieldName, dataTypeParser.stringToDataType(value));
                        }
                        lineCount++;
                        this.users.add(doc);
                    }
                }
            } finally {
                object.close();
            }
        } catch (Exception e) {
            log.info(new StringBuffer(e.getMessage()).append(e.getStackTrace()).toString());
        }

    }

}
