package com.real.matcher.service;

import com.real.matcher.Matcher;
import org.apache.commons.csv.CSVRecord;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CSVRecordMatcher {
    /**
     * Compare the given two CSV Record Set based on the provided column mappings and return a list of IdMapping
     * @param csvRecordSet1 csvRecordSet1 to iterate<br></br>
     * @param csvRecordSet2 csvRecordSet2 to compare data<br></br>
     * @param columnMapping Expects a map with keys: columnMap and resultColumnMap.<p></p>
     *                      columnMap contains Mapping of columns in csvRecordSet1 and csvRecordSet2.<p></p>
     *                      resultColumnMap contains mapping of resultant column from where ids of IDMapping(internalId, externalId) are fetched from both sets respectively
     * @return List of IdMapping if found matched records or else empty list
     */
    List<Matcher.IdMapping> matchCSVRecord(Set<CSVRecord> csvRecordSet1, Set<CSVRecord> csvRecordSet2, Map<String, Map<String, List<String>>> columnMapping);
}
