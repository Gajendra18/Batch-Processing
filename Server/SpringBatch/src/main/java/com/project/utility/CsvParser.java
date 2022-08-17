package com.project.utility;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

@Component
public class CsvParser {
    @Autowired
    MongoTemplate mongoTemplate;

    @Value("${application.bucket.name}")
    private String bucketName;

    @Value("${cloud.aws.region.static}")
    private String region;

    @Autowired
    private AmazonS3 s3Client;

    private final List<Function<String, Object>> FUNCTIONS =
            Arrays.asList(this::tryInteger, this::tryDecimal, this::tryDate);
    private Map<String,String> DATE_FORMAT_REGEXPS = new HashMap<>();

    public CsvParser(){
        DATE_FORMAT_REGEXPS.put("yyyy-MM-dd","\\d{4}-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])");
        DATE_FORMAT_REGEXPS.put("yyyy/MM/dd","\\d{4}/(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])");
        DATE_FORMAT_REGEXPS.put("MM-dd-yyyy","(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])-\\d{4}");
        DATE_FORMAT_REGEXPS.put("MM/dd/yyyy","(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\\d{4}");
        DATE_FORMAT_REGEXPS.put("dd-MM-yyyy","(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\\d{4}");
        DATE_FORMAT_REGEXPS.put("dd/MM/yyyy","(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\\d{4}");
    }


    private Object tryDate(String dateString) {
        for (Map.Entry<String, String> entry : DATE_FORMAT_REGEXPS.entrySet()) {
            if (dateString.toLowerCase().matches(entry.getValue())) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(entry.getKey());
                try {
                    return dateFormat.parse(dateString);
                } catch (ParseException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private Object tryDecimal(String value) {
        String decimalRegex = "[+-]?\\d+(\\.\\d+)?([Ee][+-]?\\d+)?";
        if (value.toLowerCase().matches(decimalRegex)) {
            try{
                return Float.parseFloat(value);
            }catch(NumberFormatException e){
                return Double.parseDouble(value);
            }
        }
        return null;
    }

    private Object tryInteger(String value) {
        String integerRegex = "[+-]?\\d+";
        if (value.toLowerCase().matches(integerRegex)) {
            try{
                return Integer.parseInt(value);
            }catch(NumberFormatException e){
                return Long.parseLong(value);
            }
        }
        return null;
    }

    public Object stringToDataType(String valueAsString) {
        return FUNCTIONS.stream()
                .map(f -> f.apply(valueAsString))
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(valueAsString);
    }

    public int countLineFast(String fileName) throws IOException {

        int lines = 0;
        System.out.println(fileName);
        S3Object object = s3Client.getObject(bucketName, fileName);
        System.out.println(object.getObjectContent());
        try (S3ObjectInputStream inputStream = object.getObjectContent();
                InputStream is = new BufferedInputStream(inputStream)) {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean endsWithoutNewLine = false;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
                endsWithoutNewLine = (c[readChars - 1] != '\n');
            }
            if (endsWithoutNewLine) {
                ++count;
            }
            lines = count;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines-1;
    }
}
