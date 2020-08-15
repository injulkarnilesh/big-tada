package org.example.stream.tweet;

import java.util.Date;
import java.util.Properties;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class TweetsFilter {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = tweeterConnectionProperties();
    DataStreamSource<String> tweetsStream = environment.addSource(new TwitterSource(properties));
//    tweetsStream.print();
//    printEnglishTweets(tweetsStream);
//    countTweetsPerLanguageInTwoMinutes(tweetsStream);
//    topTagsIn2MinutesWithCount(tweetsStream);

//    DataStream<String> controlStream = environment.socketTextStream("localhost", 9876);
//    controlLanguageFromSocket(tweetsStream, controlStream);

    environment.execute();
  }

  private static void controlLanguageFromSocket(DataStreamSource<String> tweetsStream,
      DataStream<String> controlStream) {
    SingleOutputStreamOperator<LanguageConfig> configStream = controlStream
        .flatMap(new FlatMapFunction<String, LanguageConfig>() {
          @Override
          public void flatMap(String str, Collector<LanguageConfig> collector) throws Exception {
            String[] split = str.split(";");
            for (String config : split) {
              collector.collect(LanguageConfig.parseSeparatedByEquals(config));
            }
          }
        });

    tweetsStream.map(new TweetAdapter())
        .filter(t -> t.getLang() != null)
        .keyBy(new KeySelector<Tweet, String>() {
          @Override
          public String getKey(Tweet tweet) throws Exception {
            return tweet.getLang();
          }
        }).connect(configStream.keyBy(new KeySelector<LanguageConfig, String>() {
          @Override
          public String getKey(LanguageConfig languageConfig) throws Exception {
            return languageConfig.getLang();
          }
        })).flatMap(new RichCoFlatMapFunction<Tweet, LanguageConfig, Tuple2<String, String>>() {

          ValueStateDescriptor<Boolean> shouldProcessALanguage = new ValueStateDescriptor<Boolean>("languageConfig", Boolean.class);

          @Override
          public void flatMap1(Tweet tweet, Collector<Tuple2<String, String>> collector) throws Exception {
            Boolean shouldProcess = getRuntimeContext().getState(shouldProcessALanguage).value();
            if (shouldProcess != null && shouldProcess) {
              for (String tag : tweet.tags()) {
                collector.collect(Tuple2.of(tweet.getLang(), tag));
              }
            }
          }

          @Override
          public void flatMap2(LanguageConfig languageConfig, Collector<Tuple2<String, String>> collector) throws Exception {
            getRuntimeContext().getState(shouldProcessALanguage).update(languageConfig.isShouldProcess());
          }
        }).print();
  }

  private static void topTagsIn2MinutesWithCount(DataStreamSource<String> tweetsStream) {
    tweetsStream
        .map(new TweetAdapter())
        .filter(t -> {
          Entity entities = t.getEntities();
          return entities != null && entities.getHashtags() != null
              && entities.getHashtags().size() > 0;
        }).flatMap(new FlatMapFunction<Tweet, Tuple2<String, Long>>() {
          @Override
          public void flatMap(Tweet tweet, Collector<Tuple2<String, Long>> collector) throws Exception {
            for (String tag : tweet.tags()) {
              collector.collect(Tuple2.of(tag, 1L));
            }
          }
        })
        .keyBy(0)
        .timeWindow(Time.minutes(2))
        .sum(1)
        .timeWindowAll(Time.minutes(2))
        .apply(new AllWindowFunction<Tuple2<String, Long>, Tuple3<String, Long, Date>, TimeWindow>() {
          @Override
          public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable,
              Collector<Tuple3<String, Long, Date>> collector) throws Exception {
            String maxTag = "";
            Long maxCount = 0L;
            for (Tuple2<String, Long> tagWithCount : iterable) {
              if (tagWithCount.f1 > maxCount) {
                maxCount = tagWithCount.f1;
                maxTag = tagWithCount.f0;
              }
            }
            collector.collect(Tuple3.of(maxTag, maxCount, new Date(timeWindow.getEnd())));
          }
        })
        .print();
  }

  private static void countTweetsPerLanguageInTwoMinutes(DataStreamSource<String> tweetsStream) {
    tweetsStream
        .map(new TweetAdapter())
        .filter(t -> t.getLang() != null)
        .keyBy(t -> t.getLang())
        .timeWindow(Time.minutes(2))
        .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
          @Override
          public void apply(String lang,
                  TimeWindow timeWindow,
                  Iterable<Tweet> iterable,
                  Collector<Tuple3<String, Long, Date>> collector) throws Exception {
              long timeEnd = timeWindow.getEnd();
              long count = 0;
              for (Tweet tweet : iterable) {
                count++;
              }
              collector.collect(Tuple3.of(lang, count, new Date(timeEnd)));
          }
        })
        .print();
  }

  private static void printEnglishTweets(DataStreamSource<String> tweetsStream) {
    tweetsStream
         .map(new TweetAdapter())
        .filter(t -> t.getLang() != null)
        .filter(t -> "en".equals(t.getLang()))
        .print();
  }

  private static Properties tweeterConnectionProperties() {
    Properties properties = new Properties();
    properties.setProperty(TwitterSource.CONSUMER_KEY, "HYwSZTW7mQOTMfbF8izCpbMeH");
    properties.setProperty(TwitterSource.CONSUMER_SECRET, "ZrnorTFeEOr6FB67cQnpfNcvyIFoegvWXkdg8aALtyg9K6zpRI");
    properties.setProperty(TwitterSource.TOKEN, "1229729090040516609-NSeGX7ASNLHpX61we6HLq3wviCaMb4");
    properties.setProperty(TwitterSource.TOKEN_SECRET, "sRrcLCwnrEA5Ok4ojxwsj1qvjK2Io4jvkAnLsYUYVqXG2");
    return properties;
  }
}
