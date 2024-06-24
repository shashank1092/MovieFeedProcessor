package com.real.matcher.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class CSVDataStreamProcessorImpl implements CSVDataStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CSVDataStreamProcessorImpl.class);
    private static final int BATCH_SIZE = 1000;
    public CSVDataStreamProcessorImpl() {
        //Not required
    }

    /**
     * Process the given CSV data stream and returns a Set<CSVRecord>
     * @param headerColumns String[] header
     * @param dataStream Stream<String> rows data
     * @return Set<CSVRecord>
     */
    @Override
    public Set<CSVRecord> processCsvData(String[] headerColumns, Stream<String> dataStream) {
        Set<CSVRecord> csvRecordSet = new HashSet<>();
        Set<CSVRecord> batchSet = new HashSet<>();

        // Prepare the CSV format based on columns
        CSVFormat csvFormat = CSVFormat.DEFAULT
                .builder()
                .setHeader(headerColumns)
                .build();

        //Process each data feed row and parse
        Iterator<String> iterator = dataStream.iterator();
        while(iterator.hasNext()){
            String data = iterator.next();
            Reader reader = new StringReader(data);
            try (CSVParser csvParser = csvFormat.parse(reader)) {
                for (CSVRecord csvRecord : csvParser) {
                    batchSet.add(csvRecord);
                    if (batchSet.size() >= BATCH_SIZE) {
                        csvRecordSet.addAll(batchSet);
                        batchSet.clear();
                    }
                }
                csvRecordSet.addAll(batchSet);
                batchSet.clear();
            } catch (IOException e) {
                logger.error("Error occurred while parsing data stream, cause: {}", e.getMessage());
            }

        }
        return csvRecordSet;
    }
}
