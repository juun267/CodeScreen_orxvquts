package com.real.matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MatcherImpl implements Matcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);
  private final Map<Integer, String> movies; // Store internal movie id to title mapping
  private final Map<Integer, List<String>> actorsAndDirectors; // Store movie id to list of actors and directors

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("Importing database");
    movies = new HashMap<>();
    actorsAndDirectors = new HashMap<>();
    loadMovies(movieDb);
    loadActorsAndDirectors(actorAndDirectorDb);
    LOGGER.info("Database imported");
  }

  private void loadMovies(CsvStream movieDb) {
    movieDb.getDataRows().forEach(row -> {
      String[] columns = parseMovieRow(row);
      if (columns.length == 3) {
        int id = Integer.parseInt(columns[0]);
        String title = columns[1].trim();
        movies.put(id, title);
      }
    });
  }

  private void loadActorsAndDirectors(CsvStream actorAndDirectorDb) {
    actorAndDirectorDb.getDataRows().forEach(row -> {
      String[] columns = row.split(",", -1); // -1 to include trailing empty strings
      if (columns.length == 3) {
        int movieId = Integer.parseInt(columns[0]);
        String name = columns[1].replace("\"", "").trim(); // Clean quotes
        actorsAndDirectors.computeIfAbsent(movieId, k -> new ArrayList<>()).add(name);
      }
    });
  }

  private String[] parseMovieRow(String row) {
    // Split the row by the first and last commas only
    String[] parts = row.split(",", -1);
    String id = parts[0];
    String title = row.substring(row.indexOf(",") + 1, row.lastIndexOf(",")).trim();
    String year = parts[parts.length - 1];
    return new String[]{id, title, year};
  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    List<IdMapping> mappings = new ArrayList<>();

    externalDb.getDataRows().forEach(row -> {
      String[] externalParts = row.split(",", -1);
      String mediaId = externalParts[2]; // MediaId
      String externalTitle = externalParts[3].replace("\"", "").trim(); // Title

      // Match movie title
      for (Map.Entry<Integer, String> entry : movies.entrySet()) {
        if (entry.getValue().equalsIgnoreCase(externalTitle)) {
          mappings.add(new IdMapping(entry.getKey(), mediaId));
          break; // Stop searching after finding the first match
        }
      }
    });

    return mappings;
  }
}
