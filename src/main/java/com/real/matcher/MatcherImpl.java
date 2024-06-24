package com.real.matcher;

import com.real.matcher.constant.Constants;
import com.real.matcher.service.CSVDataStreamProcessor;
import com.real.matcher.service.CSVDataStreamProcessorImpl;
import com.real.matcher.service.CSVRecordMatcher;
import com.real.matcher.service.CSVRecordMatcherImpl;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MatcherImpl implements Matcher {

  private final CsvStream movieDb;
  private final CsvStream actorAndDirectorDb;
  private final CSVRecordMatcher csvRecordMatcher = new CSVRecordMatcherImpl();
  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("importing database");
    this.movieDb=movieDb;
    this.actorAndDirectorDb=actorAndDirectorDb;
    LOGGER.info("database imported");
  }

  public CsvStream getMovieDb() {
    return movieDb;
  }

  public CsvStream getActorAndDirectorDb() {
    return actorAndDirectorDb;
  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    //Get all Header Columns
     final String[] headerColumnsForMovieFeeds = externalDb.getHeaderRow().toLowerCase().split(Constants.COMMA_DELIMITER);
     final String[] headerColumnsForMovies = movieDb.getHeaderRow().toLowerCase().split(Constants.COMMA_DELIMITER);
     final String[] headerColumnsForActorsAndDirectors = actorAndDirectorDb.getHeaderRow().toLowerCase().split(Constants.COMMA_DELIMITER);

    //Get CSVDataStreamProcessor instance
    CSVDataStreamProcessor csvDataStreamProcessor = new CSVDataStreamProcessorImpl();

    //Process movie feed data
    Set<CSVRecord> moviesFeedSet = csvDataStreamProcessor.processCsvData(headerColumnsForMovieFeeds, externalDb.getDataRows());

    //Process Movie Data
    Set<CSVRecord> moviesSet = csvDataStreamProcessor.processCsvData(headerColumnsForMovies, movieDb.getDataRows());

    //Match Movie data with movie feed
    List<IdMapping> matchedMoviesBasedOnMovieDB = getMatchedDataFromMovieDatabase(moviesSet, moviesFeedSet);
    Set<IdMapping> resultSet = new HashSet<>(matchedMoviesBasedOnMovieDB);
    //Clear processed data to claim heap space
    moviesSet.clear();
    matchedMoviesBasedOnMovieDB.clear();

    //Process Actors and directors data
    Set<CSVRecord> actorsAndDirectorsSet = csvDataStreamProcessor.processCsvData(headerColumnsForActorsAndDirectors, actorAndDirectorDb.getDataRows());

    //Match Actor and Directors data with movie feed
    List<IdMapping> matchedMoviesBasedOnActorAndDirectorsDB = getMatchedDataFromActorAndDirectorsDatabase(actorsAndDirectorsSet, moviesFeedSet);
    resultSet.addAll(matchedMoviesBasedOnActorAndDirectorsDB);

    //Clear Processed Data
    moviesFeedSet.clear();
    actorsAndDirectorsSet.clear();
    matchedMoviesBasedOnActorAndDirectorsDB.clear();

    return new ArrayList<>(resultSet);
  }

  /**
   * Get IdMapping of matched records based on the Movie Database
   * @param moviesSet
   * @param moviesFeedSet
   * @return IdMapping list
   */
  private List<IdMapping> getMatchedDataFromMovieDatabase(Set<CSVRecord> moviesSet, Set<CSVRecord> moviesFeedSet) {
    //Create Column mapping for Movie data and movie feed columns
    Map<String, List<String>> movieToMovieFeedColumnMapping = new HashMap<>();
    movieToMovieFeedColumnMapping.put(Constants.TITLE, List.of(Constants.TITLE));
    //movieToMovieFeedColumnMapping.put(Constants.YEAR, List.of(Constants.ORIGINAL_RELEASE_DATE));

    //Create result Column mapping for Movie id and movie feed
    Map<String, List<String>> movieToMovieFeedResultColumnMapping = new HashMap<>();
    movieToMovieFeedResultColumnMapping.put(Constants.MEDIA_ID, List.of(Constants.ID));

    Map<String, Map<String, List<String>>> movieToMovieFeedMap = new HashMap<>();
    movieToMovieFeedMap.put(Constants.COLUMN_MAP, movieToMovieFeedColumnMapping);
    movieToMovieFeedMap.put(Constants.RESULT_COLUMN_MAP, movieToMovieFeedResultColumnMapping);
    return csvRecordMatcher.matchCSVRecord(moviesFeedSet, moviesSet, movieToMovieFeedMap);
  }

  /**
   * Get IdMapping of matched records based on the Actor and directors Database
   * @param actorsAndDirectorsSet
   * @param moviesFeedSet
   * @return IdMapping list
   */
  private List<IdMapping> getMatchedDataFromActorAndDirectorsDatabase(Set<CSVRecord> actorsAndDirectorsSet, Set<CSVRecord> moviesFeedSet) {
    //Create Column mapping for Actor/Director data and movie feed columns
    Map<String, List<String>> actorAndDirectorToMovieFeedColumnMapping = new HashMap<>();
    actorAndDirectorToMovieFeedColumnMapping.put(Constants.ACTORS, List.of(Constants.ACTOR_DIRECTOR_NAME));
    actorAndDirectorToMovieFeedColumnMapping.put(Constants.DIRECTOR, List.of(Constants.ACTOR_DIRECTOR_NAME));

    //Create result Column mapping for Movie id and movie feed
    Map<String, List<String>> actorAndDirectorToMovieFeedResultColumnMapping = new HashMap<>();
    actorAndDirectorToMovieFeedResultColumnMapping.put(Constants.MEDIA_ID, List.of(Constants.MOVIE_ID));

    Map<String, Map<String, List<String>>> actorAndDirectorToMovieFeedMap = new HashMap<>();
    actorAndDirectorToMovieFeedMap.put(Constants.COLUMN_MAP, actorAndDirectorToMovieFeedColumnMapping);
    actorAndDirectorToMovieFeedMap.put(Constants.RESULT_COLUMN_MAP, actorAndDirectorToMovieFeedResultColumnMapping);
    return csvRecordMatcher.matchCSVRecord(moviesFeedSet, actorsAndDirectorsSet, actorAndDirectorToMovieFeedMap);
  }

}
