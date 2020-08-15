package org.example.stream.tweet;

import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Entity {
  private List<HashTag> hashtags;

  public List<HashTag> getHashtags() {
    return hashtags;
  }

  public void setHashtags(List<HashTag> hashtags) {
    this.hashtags = hashtags;
  }
}
