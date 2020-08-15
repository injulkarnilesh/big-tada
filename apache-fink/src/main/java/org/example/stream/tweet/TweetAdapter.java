package org.example.stream.tweet;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class TweetAdapter implements MapFunction<String, Tweet> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public Tweet map(String s) throws Exception {
    return OBJECT_MAPPER.readValue(s, Tweet.class);
  }
}
