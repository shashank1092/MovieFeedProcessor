package com.real.matcher.service;

import org.apache.commons.csv.CSVRecord;

import java.util.Set;
import java.util.stream.Stream;

public interface CSVDataStreamProcessor {
    public Set<CSVRecord> processCsvData(String[] headerColumns, Stream<String> dataStream);
}
