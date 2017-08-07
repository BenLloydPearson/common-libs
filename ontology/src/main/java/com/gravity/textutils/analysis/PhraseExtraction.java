package com.gravity.textutils.analysis;

import java.util.List;

public class PhraseExtraction {
  private String[] phraseArray;

  public PhraseExtraction(String[] phraseArray) {
    this.phraseArray = phraseArray;
  }

  @Deprecated
  public PhraseExtraction(List<String> phraseArray) {
    this.phraseArray = (String[]) phraseArray.toArray();
  }

  public boolean isSignificant() {
    return !this.toString().matches("/^[^a-zA-Z]*$/");
  }

  public String[] getPhrases() {
    return phraseArray;
  }

  @Override
  public String toString() {
    String buffer = "";
    for (String phrase : phraseArray) buffer += phrase + " ";
    buffer = buffer.trim();
    buffer = buffer.replace(" '", "'");
    buffer = buffer.replace(" .", ".");
    return buffer;
  }
}
