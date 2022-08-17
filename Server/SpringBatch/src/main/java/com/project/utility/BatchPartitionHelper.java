package com.project.utility;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
@Slf4j
public class BatchPartitionHelper {

    @Autowired
    private CsvParser csvParser;

    public String[] getAllFiles(String fileName) {
        String[] contents = fileName.split(",");
        for (String con : contents) {
            log.info("Content : " + con);
        }
        return contents;
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


    public List<Map<String, Integer>> getStartingIndex(String fileName, int partitionSize) {
        int numberOfLines = 0;
        try {
            numberOfLines = csvParser.countLineFast(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int linesPerWorker = (int) Math.ceil(numberOfLines / (double) partitionSize);
        List<Map<String, Integer>> index = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("startingIndex", (linesPerWorker * i) + 2);
            map.put("endingIndex", map.get("startingIndex") + linesPerWorker - 1);
            index.add(map);
        }
        return index;
    }





}
