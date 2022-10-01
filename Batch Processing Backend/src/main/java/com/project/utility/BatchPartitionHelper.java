package com.project.utility;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.project.exception.BatchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;

@Component
@Slf4j
public class BatchPartitionHelper {

    @Value("${application.bucket.name}")
    private String bucketName;


    @Autowired
    private AmazonS3 s3Client;

    public String[] getAllFiles(String fileName) {
        return fileName.split(",");
    }


    public List<List<String>> splitPayLoad(String[] array, int partitionSize) {
        if (partitionSize <= 0)
            return Collections.emptyList();
        int rest = array.length % partitionSize;
        int chunks = array.length / partitionSize + (rest > 0 ? 1 : 0);
        String[][] arrays = new String[chunks][];
        for (int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++) {
            arrays[i] = Arrays.copyOfRange(array, i * partitionSize, i * partitionSize + partitionSize);
        }
        if (rest > 0) {
            arrays[chunks - 1] = Arrays.copyOfRange(array, (chunks - 1) * partitionSize,
                    (chunks - 1) * partitionSize + rest);
        }
        List<List<String>> list = new ArrayList<>();
        for (String[] arr : arrays) {
            list.add(Arrays.asList(arr));
        }
        return list;
    }


    public List<Map<String, Integer>> getStartingIndex(String fileName, int partitionSize) throws BatchException {
        int numberOfLines = 0;
        try {
            numberOfLines = countLineFast(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int linesPerWorker = (int) Math.ceil(numberOfLines / (double) partitionSize);
        if (linesPerWorker == 0) return null;
        List<Map<String, Integer>> index = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("startingIndex", (linesPerWorker * i) + 2);
            map.put("endingIndex", map.get("startingIndex") + linesPerWorker - 1);
            index.add(map);
        }
        return index;
    }

    public List<Map<String, Integer>> getStartingIndex(MultipartFile file, int partitionSize) throws BatchException {
        int numberOfLines = 0;
        try {
            numberOfLines = countLineFast(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int linesPerWorker = (int) Math.ceil(numberOfLines / (double) partitionSize);
        if (linesPerWorker == 0) return null;
        List<Map<String, Integer>> index = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("startingIndex", (linesPerWorker * i) + 2);
            map.put("endingIndex", map.get("startingIndex") + linesPerWorker - 1);
            index.add(map);
        }
        return index;
    }


    public int countLineFast(String fileName) throws IOException, BatchException {
        try (S3Object object = s3Client.getObject(bucketName, fileName)) {
            try (S3ObjectInputStream inputStream = object.getObjectContent();
                 InputStream is = new BufferedInputStream(inputStream)) {
                if (getFileType(fileName).equals("CSV")) {
                    return getCSVRowCount(is);
                } else {
                    return getExcelRowCount(is);
                }
            } finally {
                object.close();
            }
        } catch (IOException e) {
            throw new BatchException(e.getMessage());
        }
    }


    public int countLineFast(MultipartFile file) throws IOException, BatchException {
        try (InputStream inputStream = file.getInputStream();) {
                if (getFileType(file.getOriginalFilename()).equals("CSV")) {
                    return getCSVRowCount(inputStream);
                } else {
                    return getExcelRowCount(inputStream);
                }
            } catch (IOException e) {
            throw new BatchException(e.getMessage());
            }
    }




    public int getCSVRowCount(InputStream inputStream) throws BatchException {
        int lines;
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean endsWithoutNewLine = false;
            while ((readChars = inputStream.read(c)) != -1) {
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
            throw new BatchException(e.getMessage());
        }
        return lines - 1 >= 1 ? lines - 1 : 0;
    }


    public int getExcelRowCount(InputStream inputStream) throws IOException {
        XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
        XSSFSheet sheet = workbook.getSheetAt(0);
        int index = workbook.getSheetIndex(sheet);
        if (index == -1)
            return 0;
        else {
            return sheet.getLastRowNum();
        }
    }

    public String getFileType(String fileName) {
        if (fileName.endsWith(".csv") || fileName.endsWith(".CSV")) return "CSV";
        else return "EXCEL";
    }




}
