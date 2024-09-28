package com.real.matcher;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MatcherImpl implements Matcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MatcherImpl.class);

  private final List<Movie> movies;
  private final List<ActorAndDirector> actorsAndDirectors;

  public MatcherImpl(CsvStream movieDb, CsvStream actorAndDirectorDb) {
    LOGGER.info("importing database");
    // TODO implement me
    this.movies = parseMovies(movieDb);
    this.actorsAndDirectors = parseActorsAndDirectors(actorAndDirectorDb);
    LOGGER.info("database imported");
  }

  // Parse movies CSV into list of Movie objects
  private List<Movie> parseMovies(CsvStream csv) {
    return csv.getDataRows().map(row -> {
      String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
      int id = Integer.parseInt(columns[0].trim()); // id
      String title = columns[1].trim().replaceAll("\"", ""); // title, remove quotes
      int year;

      // Check for NULL or invalid year value
      String yearStr = columns[2].trim();
      if ("NULL".equals(yearStr) || yearStr.isEmpty()) {
        year = 0; // or throw an exception, or handle accordingly
      } else {
        year = Integer.parseInt(yearStr);
      }

      return new Movie(id, title, year);
    }).collect(Collectors.toList());
  }


  // Parse actors and directors CSV into list of ActorAndDirector objects
  private List<ActorAndDirector> parseActorsAndDirectors(CsvStream csv) {
    return csv.getDataRows().map(row -> {
      String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // Split considering quoted fields
      return new ActorAndDirector(
              Integer.parseInt(columns[0].trim()),   // movie_id
              columns[1].trim().replaceAll("\"", ""), // name, remove quotes
              columns[2].trim()                        // role (cast or director)
      );
    }).collect(Collectors.toList());
  }

  @Override
  public List<IdMapping> match(DatabaseType databaseType, CsvStream externalDb) {
    List<IdMapping> idMappings = new ArrayList<>();

    externalDb.getDataRows().forEach(row -> {
      String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // Split considering quoted fields
      String externalId = columns[2].trim();      // MediaId
      String externalTitle = columns[3].trim();   // Title
      String externalDate = columns[4].trim();    // OriginalReleaseDate

      // Validate date format before extracting year
      int externalYear = extractYear(externalDate); // Extract year from date

      Optional<Movie> matchedMovie = movies.stream()
              .filter(movie -> movie.getTitle().equalsIgnoreCase(externalTitle) && movie.getYear() == externalYear)
              .findFirst();

      matchedMovie.ifPresent(movie -> idMappings.add(new IdMapping(movie.getId(), externalId))); // Corrected line
    });

    return idMappings;
  }

  private int extractYear(String date) {
    if (date == null || date.isEmpty()) {
      LOGGER.error("Provided date is null or empty");
      return 0; // Handle as needed
    }
    try {
      LocalDate parsedDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a"));
      return parsedDate.getYear();
    } catch (DateTimeParseException e) {
      LOGGER.error("Invalid date format: " + date, e);
      return 0; // Handle as needed
    }
  }


  // Find a movie by matching the normalized title and year
  private Optional<Movie> findMatchingMovie(String externalTitle, int externalYear) {
    String normalizedExternalTitle = normalizeTitle(externalTitle);

    // Basic matching on title and year
    List<Movie> potentialMatches = movies.stream()
            .filter(movie -> normalizeTitle(movie.getTitle()).equals(normalizedExternalTitle))
            .filter(movie -> movie.getYear() == externalYear)
            .collect(Collectors.toList());

    // If multiple matches, refine using actors and director information
    if (potentialMatches.size() > 1) {
      return refineMatchWithActorsAndDirector(externalTitle, externalYear, potentialMatches);
    }

    return potentialMatches.isEmpty() ? Optional.empty() : Optional.of(potentialMatches.get(0));
  }

  // Normalize title by converting to lowercase and removing non-alphanumeric characters
  private String normalizeTitle(String title) {
    return title.toLowerCase().replaceAll("[^a-z0-9]", "");
  }

  // Refine the match using actors and director information
  private Optional<Movie> refineMatchWithActorsAndDirector(String externalActors, int externalYear, List<Movie> potentialMatches) {
    for (Movie movie : potentialMatches) {
      List<String> movieActors = getActorsForMovie(movie.getId());
      String movieDirector = getDirectorForMovie(movie.getId());

      // Match actors or director
      if (movieActors.stream().anyMatch(externalActors::contains) || movieDirector.equals(externalActors)) {
        return Optional.of(movie);
      }
    }
    return Optional.empty();
  }

  // Get actors for a movie by its ID
  private List<String> getActorsForMovie(int movieId) {
    return actorsAndDirectors.stream()
            .filter(ad -> ad.getMovieId() == movieId && ad.getRole().equals("cast"))
            .map(ActorAndDirector::getName)
            .collect(Collectors.toList());
  }

  // Get the director for a movie by its ID
  private String getDirectorForMovie(int movieId) {
    return actorsAndDirectors.stream()
            .filter(ad -> ad.getMovieId() == movieId && ad.getRole().equals("director"))
            .map(ActorAndDirector::getName)
            .findFirst()
            .orElse("");
  }

  // Inner classes for Movie and ActorAndDirector
  private static class Movie {
    private final int id;
    private final String title;
    private final int year;

    public Movie(int id, String title, int year) {
      this.id = id;
      this.title = title;
      this.year = year;
    }

    public int getId() {
      return id;
    }

    public String getTitle() {
      return title;
    }

    public int getYear() {
      return year;
    }
  }

  private static class ActorAndDirector {
    private final int movieId;
    private final String name;
    private final String role; // "cast" or "director"

    public ActorAndDirector(int movieId, String name, String role) {
      this.movieId = movieId;
      this.name = name;
      this.role = role;
    }

    public int getMovieId() {
      return movieId;
    }

    public String getName() {
      return name;
    }

    public String getRole() {
      return role;
    }
  }
}
