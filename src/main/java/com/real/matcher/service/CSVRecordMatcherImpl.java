package com.real.matcher.service;

import com.real.matcher.Matcher;
import com.real.matcher.constant.Constants;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CSVRecordMatcherImpl implements CSVRecordMatcher{

    Logger logger = LoggerFactory.getLogger(CSVRecordMatcherImpl.class);

    @Override
    public List<Matcher.IdMapping> matchCSVRecord(Set<CSVRecord> csvRecordSet1, Set<CSVRecord> csvRecordSet2, Map<String, Map<String, List<String>>> columnMapping) {
        logger.info("CSV Record Matching started for columns : {}", columnMapping.get(Constants.COLUMN_MAP));
        List<Matcher.IdMapping> resultList = new CopyOnWriteArrayList<>();
        Map<String, List<String>> columnMap = columnMapping.get(Constants.COLUMN_MAP);
        AtomicInteger processedCount = new AtomicInteger(0);

        // Build a map for quick lookup of csvRecordSet2
        Map<String, List<CSVRecord>> record2LookupMap = buildRecordLookupMap(csvRecordSet2, columnMap);

        csvRecordSet1.parallelStream().forEach(record1-> {
            processedCount.incrementAndGet();
            //Create Predicate to match column data
            Predicate<CSVRecord> matchPredicates = createMatchPredicate(columnMap, record1);

            // Get matched records and map to IdMapping
            resultList.addAll(getIdMappingForMatchedRecord(record2LookupMap, columnMapping, record1, matchPredicates));
        });
        logger.info("Total Processed data count: {}", processedCount.get());
        return resultList.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Builds Record lookup map for movie feed db where key will be given column list
     * @param csvRecordSet
     * @param columnMap
     * @return A map containing the csv record as value and key as columns provided.
     * eg. key : title value: csvRecord
     */
    private Map<String, List<CSVRecord>> buildRecordLookupMap(Set<CSVRecord> csvRecordSet, Map<String, List<String>> columnMap) {
        logger.info("Started Building Lookup Map for Movie Feed for columns {}", columnMap);
        Map<String, List<CSVRecord>> lookupMap = new ConcurrentHashMap<>();
        for(CSVRecord csvRecord : csvRecordSet) {
            for (Map.Entry<String, List<String>> entry : columnMap.entrySet()) {
                for(String key : entry.getValue()){
                    String value = csvRecord.get(key).toLowerCase().trim();
                    computeLookupMap(csvRecord, key, value, lookupMap);
                }
            }
        }
        logger.info("Completed Building Lookup Map for Movie Feed for columns {}", columnMap);
        return lookupMap;
    }

    /**
     * Compute lookup based on column
     * eg. If column is actor, and multiple actors are present then consider all actors as separate keys
     * For all other columns consider value as single value
     * @param csvRecord
     * @param key
     * @param value
     * @param lookupMap
     */
    private static void computeLookupMap(CSVRecord csvRecord, String key, String value, Map<String, List<CSVRecord>> lookupMap) {
        //Handle Actors column in movie feed containing more than one actor
        if(key.equalsIgnoreCase(Constants.ACTORS)){
            List<String> values = List.of(value.split(Constants.COMMA_DELIMITER));
            for(String actorName : values){
                lookupMap.computeIfAbsent(actorName, k -> new ArrayList<>()).add(csvRecord);
            }
        }
        //Handle date column in movie feed
        else {
            if(key.equals(Constants.ORIGINAL_RELEASE_DATE)){
            value = value.split(" ")[0];
            DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                    .appendOptional(DateTimeFormatter.ofPattern("M/d/yyyy"))
                    .appendOptional(DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm"))
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter();
            value = String.valueOf(LocalDate.parse(value, formatter).getYear());
        }
        lookupMap.computeIfAbsent(value, k -> new ArrayList<>()).add(csvRecord);
        }
    }

    private Predicate<CSVRecord> createMatchPredicate(Map<String, List<String>> columnMap, CSVRecord record1) {
        //logger.info("Creating Predicate to filter matched data.");
        return columnMap.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream()
                        .map(columnNameSet2 -> getCsvRecordPredicate(columnNameSet2, record1.get(entry.getKey().toLowerCase()).toLowerCase())))
                .reduce(Predicate::or)
                .orElse(null);
    }

    /**
     * Get Predicate to apply to filter records based on given column and values to match to the movie feed
     * @param columnNameSet2
     * @param record1ColumnValueInLowerCase
     * @return
     */
    private Predicate<CSVRecord> getCsvRecordPredicate(String columnNameSet2, String record1ColumnValueInLowerCase) {
        return record2 -> {
            String record2ColumnValue = record2.get(columnNameSet2);
            //Search in all studio types
            return record2ColumnValue != null && (record2ColumnValue.toLowerCase().trim().contains(record1ColumnValueInLowerCase.trim()));
            //Search in studio type Xbox only
            //return record2ColumnValue != null && (record2ColumnValue.toLowerCase().trim().contains(record1ColumnValueInLowerCase.trim())) && record2.get(Constants.STUDIO_NETWORK).toLowerCase().trim().contains(Matcher.DatabaseType.XBOX.name().toLowerCase());
        };
    }


    /**
     * Map matched record to IdMapping instance
     * @param record2LookupMap
     * @param columnMapping
     * @param record1
     * @param matchPredicates
     * @return Set<IdMapping>
     */
    private Set<Matcher.IdMapping> getIdMappingForMatchedRecord(Map<String, List<CSVRecord>> record2LookupMap, Map<String, Map<String, List<String>>> columnMapping, CSVRecord record1, Predicate<CSVRecord> matchPredicates) {
        //logger.info("Started Matching record and mapping to IdMapping.");
        if (matchPredicates == null) return Collections.emptySet();

        String resultColumnSet1 = columnMapping.get(Constants.RESULT_COLUMN_MAP).keySet().iterator().next();
        String resultColumnSet2 = columnMapping.get(Constants.RESULT_COLUMN_MAP).get(resultColumnSet1).get(0);
        List<CSVRecord> potentialMatches = new CopyOnWriteArrayList<>();
        //Get all Potential matches based on the Column values as key
        columnMapping.get(Constants.COLUMN_MAP).forEach((columnNameSet1, columnNamesListSet2) -> columnNamesListSet2.forEach(columnNameSet2 -> {
            potentialMatches.addAll(record2LookupMap.getOrDefault(record1.get(columnNameSet1).toLowerCase().trim(), Collections.emptyList()));
        }));

        return  potentialMatches.parallelStream()
                .filter(matchPredicates)
                .map(record2 -> new Matcher.IdMapping(Integer.parseInt(record2.get(resultColumnSet2)),
                        record1.get(resultColumnSet1)))
                .collect(Collectors.toSet());
    }
}
