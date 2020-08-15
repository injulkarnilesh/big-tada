package org.example.stream.tweet;

public class LanguageConfig {
  private String lang;
  private boolean shouldProcess;

  public LanguageConfig(String lang, boolean shouldProcess) {
    this.lang = lang;
    this.shouldProcess = shouldProcess;
  }

  public static LanguageConfig parseSeparatedByEquals(String str) {
    String[] split = str.split("=");
    return new LanguageConfig(split[0], Boolean.parseBoolean(split[1]));
  }

  public String getLang() {
    return lang;
  }

  public boolean isShouldProcess() {
    return shouldProcess;
  }

  @Override
  public String toString() {
    return "LanguageConfig{" +
        "lang='" + lang + '\'' +
        ", shouldProcess=" + shouldProcess +
        '}';
  }
}
