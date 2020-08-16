package org.example.stream.cep;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.stream.tweet.LanguageConfig;

public class SimpleTextCEP {

  /*
  $nc -l localhost 9876
   */
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<String> controlStream = environment.socketTextStream("localhost", 9876);
    SingleOutputStreamOperator<Event> eventStream = controlStream
        .flatMap(new FlatMapFunction<String, Event>() {
          @Override
          public void flatMap(String str, Collector<Event> collector) throws Exception {
            String[] split = str.split("\\s");
            for (String event : split) {
              collector.collect(new Event(event));
            }
          }
        });

    Pattern<Event, Event> abFollowedByC = Pattern.<Event>begin("start")
        .where(new SimpleCondition<Event>() {
          @Override
          public boolean filter(Event event) throws Exception {
            return event.value.startsWith("a");
          }
        })
        .next("middle").where(new SimpleCondition<Event>() {
          @Override
          public boolean filter(Event event) throws Exception {
            return event.value.startsWith("b");
          }
        }).followedBy("end")
        .where(new SimpleCondition<Event>() {
          @Override
          public boolean filter(Event event) throws Exception {
            return event.value.startsWith("c");
          }
        });

    PatternStream<Event> patternStream = CEP.pattern(eventStream, abFollowedByC);
    SingleOutputStreamOperator<MatchingEvents> matching = patternStream
        .select(new PatternSelectFunction<Event, MatchingEvents>() {
          @Override
          public MatchingEvents select(Map<String, List<Event>> map) throws Exception {
            List<Event> events = map.values().stream().flatMap(Collection::stream)
                .collect(Collectors.toList());
            return new MatchingEvents(events);
          }
        });

    matching.print();

    environment.execute();

  }

  public static class Event {
    private final String value;

    public Event(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "Event{" +
          "value='" + value + '\'' +
          '}';
    }
  }

  public static class MatchingEvents {
    private List<Event> events;

    public MatchingEvents(List<Event> events) {
      this.events = events;
    }

    public List<Event> getEvents() {
      return events;
    }

    @Override
    public String toString() {
      return "MatchingEvents{" +
          "events=" + events +
          '}';
    }
  }

}
