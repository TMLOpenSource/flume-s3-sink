package com.tml.sinks;

import java.util.Calendar;

public class FileNameIdentifier {

  private String identifier;

  FileNameIdentifier() {
    generateNewIdentifier();
  }

  public String getIdentifier() {
    return identifier;
  }

  public void generateNewIdentifier() {
    this.identifier = String.valueOf(Calendar.getInstance().getTimeInMillis());
  }
}
