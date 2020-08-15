package org.example.stream.tweet;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {
    private String lang;
    private String text;
    private Entity entities;

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Entity getEntities() {
    return entities;
  }

  public void setEntities(Entity entities) {
    this.entities = entities;
  }

  public List<String> tags() {
    return entities == null ? Arrays.asList() :
        entities.getHashtags() == null ? Arrays.asList()
            : entities.getHashtags().stream().map(HashTag::getText).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "Tweet{" +
        "lang='" + lang + '\'' +
        ", text='" + text + '\'' +
        ", tags='" + tags() + '\'' +
        '}';
  }
}
