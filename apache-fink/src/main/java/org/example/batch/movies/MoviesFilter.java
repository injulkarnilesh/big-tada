package org.example.batch.movies;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class MoviesFilter {

  //--input C:\personal-workspace\apache-fink\src\main\resources\ml-latest-small\movies.csv --output C:\personal-workspace\apache-fink\src\main\resources\ml-latest-small\drama-movies
  public static final String OUT_PUT_PATH = "C:\\personal-workspace\\apache-fink\\src\\main\\resources\\ml-latest-small\\drama-movies";
  public static final String RATINGS_FILE = "C:\\personal-workspace\\apache-fink\\src\\main\\resources\\ml-latest-small\\ratings.csv";
  public static final String MOVIES_FILE = "C:\\personal-workspace\\apache-fink\\src\\main\\resources\\ml-latest-small\\movies.csv";

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String moviesFile = parameterTool.get("input");
    String outputFile = parameterTool.get("output");

    ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
    findMoviesByGenre(executionEnvironment, "Drama", moviesFile, outputFile);
//    ratingsByGenre(executionEnvironment);
//    findTopMovies(executionEnvironment, 10);
  }

  private static void findTopMovies(ExecutionEnvironment executionEnvironment, int topCount) throws Exception {
    DataSource<Tuple2<Long, Double>> ratings = readRatings(executionEnvironment);
    MapPartitionOperator<Tuple2<Long, Double>, Tuple2<Long, Double>> top10Movies =
        ratings
        .groupBy(0)
        .reduceGroup(averageRatingByMovieId())
        .partitionCustom(new Partitioner<Long>() {
              @Override
              public int partition(Long movieId, int totalPartitions) {
                return Long.valueOf(movieId % totalPartitions).intValue();
              }
        }, 0)
        .setParallelism(5)
        .sortPartition(1, Order.DESCENDING)
        .mapPartition(top(topCount))
        .sortPartition(1, Order.DESCENDING)
        .setParallelism(1)
        .mapPartition(top(topCount));

    DataSource<Tuple3<Long, String, String>> movies = readMovies(executionEnvironment, MOVIES_FILE);

    top10Movies.join(movies)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<Tuple2<Long, Double>, Tuple3<Long, String, String>, Tuple2<StringValue, DoubleValue>>() {
          private StringValue nameValue = new StringValue();
          private DoubleValue ratingValue = new DoubleValue();
          private Tuple2<StringValue, DoubleValue> returnTuple = Tuple2.of(nameValue, ratingValue);
          @Override
          public Tuple2<StringValue, DoubleValue> join(Tuple2<Long, Double> ratingTuple,
              Tuple3<Long, String, String> movieTuple) throws Exception {
            nameValue.setValue(movieTuple.f1);
            ratingValue.setValue(ratingTuple.f1);
            return returnTuple;
          }
        }).print();
  }

  private static GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> averageRatingByMovieId() {
    return new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
      @Override
      public void reduce(Iterable<Tuple2<Long, Double>> iterable,
          Collector<Tuple2<Long, Double>> collector) throws Exception {
        int count = 0;
        double totalRating = 0d;
        Long movieId = null;
        for (Tuple2<Long, Double> rating : iterable) {
          movieId = rating.f0;
          Double ratingNumber = rating.f1;
          totalRating += ratingNumber;
          count++;
        }
        if (movieId != null && count > 50) {
          collector.collect(Tuple2.of(movieId, totalRating / count));
        }
      }
    };
  }

  private static MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> top(final int topCount) {
    return new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
      @Override
      public void mapPartition(Iterable<Tuple2<Long, Double>> iterable, Collector<Tuple2<Long, Double>> collector) throws Exception {
        Iterator<Tuple2<Long, Double>> iterator = iterable.iterator();
        for (int i = 0; i < topCount && iterator.hasNext(); i++) {
          Tuple2<Long, Double> top = iterator.next();
          collector.collect(top);
        }
      }
    };
  }

  private static void ratingsByGenre(ExecutionEnvironment executionEnvironment) throws Exception {
    DataSource<Tuple3<Long, String, String>> movies = readMovies(executionEnvironment, MOVIES_FILE);
    DataSource<Tuple2<Long, Double>> ratings = readRatings(executionEnvironment);

    movies
      .map(byEachGenre())
      .flatMap(eachItem())
      .join(ratings)
      .where(0)
      .equalTo(0)
      .with(joinSelector())
      .groupBy(1)
      .reduceGroup(averageRating())
      .print();
  }

  private static DataSource<Tuple2<Long, Double>> readRatings(
      ExecutionEnvironment executionEnvironment) {
    return readyForCSV(executionEnvironment.readCsvFile(RATINGS_FILE))
        .includeFields(false, true, true, false)
        .types(Long.class, Double.class);
  }

  private static DataSource<Tuple3<Long, String, String>> readMovies(ExecutionEnvironment executionEnvironment, String moviesFile) {
    return readyForCSV(executionEnvironment.readCsvFile(moviesFile))
        .types(Long.class, String.class, String.class);
  }

  private static GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>> averageRating() {
    return new GroupReduceFunction<Tuple3<StringValue, StringValue, DoubleValue>, Tuple2<String, Double>>() {
      @Override
      public void reduce(Iterable<Tuple3<StringValue, StringValue, DoubleValue>> iterable,
          Collector<Tuple2<String, Double>> collector) throws Exception {
        int count = 0;
        Double sum = 0D;
        String genre = null;
        for (Tuple3<StringValue, StringValue, DoubleValue> movie : iterable) {
          genre = movie.f1.getValue();
          Double rating = movie.f2.getValue();
          sum += rating;
          count++;
        }
        if (genre != null) {
          collector.collect(Tuple2.of(genre, sum / count));
        }
      }
    };
  }

  private static JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<StringValue, StringValue, DoubleValue>> joinSelector() {
    return new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, Tuple3<StringValue, StringValue, DoubleValue>>() {
      private final StringValue nameValue = new StringValue();
      private final StringValue genreValue = new StringValue();
      private final DoubleValue ratingValue = new DoubleValue();
      private final Tuple3<StringValue, StringValue, DoubleValue> returnTuple = new Tuple3<>(nameValue, genreValue, ratingValue);
      @Override
      public Tuple3<StringValue, StringValue, DoubleValue> join(
          Tuple3<Long, String, String> movieTuple,
          Tuple2<Long, Double> ratingTuple) throws Exception {
        nameValue.setValue(movieTuple.f1);
        genreValue.setValue(movieTuple.f2);
        ratingValue.setValue(ratingTuple.f1);
        return returnTuple;
      }
    };
  }

  private static FlatMapFunction<List<Tuple3<Long, String, String>>, Tuple3<Long, String, String>> eachItem() {
    return new FlatMapFunction<List<Tuple3<Long, String, String>>, Tuple3<Long, String, String>>() {
      @Override
      public void flatMap(List<Tuple3<Long, String, String>> tuple3s,
          Collector<Tuple3<Long, String, String>> collector) throws Exception {
        tuple3s.forEach(tuple -> collector.collect(tuple));
      }
    };
  }

  private static MapFunction<Tuple3<Long, String, String>, List<Tuple3<Long, String, String>>> byEachGenre() {
    return new MapFunction<Tuple3<Long, String, String>, List<Tuple3<Long, String, String>>>() {
      @Override
      public List<Tuple3<Long, String, String>> map(Tuple3<Long, String, String> movieTuple)
          throws Exception {
        String genre = movieTuple.f2;
        String movieName = movieTuple.f1;
        Long id = movieTuple.f0;
        String[] genres = genre.split("\\|");
        List<Tuple3<Long, String, String>> tuples = Arrays.stream(genres)
            .map(g -> Tuple3.of(id, movieName, g))
            .collect(Collectors.toList());
        return tuples;
      }
    };
  }

  private static CsvReader readyForCSV(CsvReader csvReader) {
      return csvReader.ignoreFirstLine()
        .ignoreInvalidLines()
        .parseQuotedStrings('"');
  }

  private static void findMoviesByGenre(ExecutionEnvironment executionEnvironment, String genre,
      String moviesFile, String outPutPath) throws Exception {
    DataSource<Tuple3<Long, String, String>> dataSource = readMovies(executionEnvironment,
        moviesFile);
    dataSource.map(readTuple())
        .filter(ofGenre(genre))
        //.print();
    .writeAsText(outPutPath);
    executionEnvironment.execute();
  }

  private static FilterFunction<Movie> ofGenre(String genre) {
    return new FilterFunction<Movie>() {
      @Override
        public boolean filter(Movie movie) throws Exception {
          return movie.getGenres().contains(genre);
        }
    };
  }

  private static MapFunction<Tuple3<Long, String, String>, Movie> readTuple() {
    return new MapFunction<Tuple3<Long, String, String>, Movie>() {
      @Override
      public Movie map(Tuple3<Long, String, String> tuple) throws Exception {
        String name = tuple.f1;
        Set<String> genres = Arrays.stream(tuple.f2.split("\\|"))
            .collect(Collectors.toSet());
        return new Movie(name, genres);
      }
    };
  }

  static class Movie {
    private final String name;
    private final Set<String> genres;

    public Movie(String name, Set<String> genres) {
      this.name = name;
      this.genres = genres;
    }

    public String getName() {
      return name;
    }

    public Set<String> getGenres() {
      return genres;
    }

    @Override
    public String toString() {
      return "Movie{" +
          "name='" + name + '\'' +
          ", genres=" + genres +
          '}';
    }
  }

}
