package com.gravity.utilities;
/**
 * Created by IntelliJ IDEA.
 * User: robbie
 * Date: Jun 14, 2010
 * Time: 5:46:48 PM
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stopwatch {
  private static Logger logger = LoggerFactory.getLogger(Stopwatch.class);

  private enum State { IDLE, STARTED, STOPPED }

  private State state = State.IDLE;

  private long start;
  private long duration = 0;
  private String formattedDuration = null;
  private boolean suppressInfo;

  public Stopwatch() {
    suppressInfo = false;
  }

  public Stopwatch(boolean suppressInfoMessages) {
    suppressInfo = suppressInfoMessages;
  }

  public long getDuration() {
    return duration;
  }

  public void start() {
    duration = 0;
    start = System.currentTimeMillis();
    state = State.STARTED;
    if (logger.isInfoEnabled() && !suppressInfo) {
      logger.info("Started at " + start + " milliseconds");
    }
  }

  public void stop() {
    long stop = System.currentTimeMillis();
    duration = stop - start;
    state = State.STOPPED;
    if (logger.isInfoEnabled() && !suppressInfo) {
      logger.info("Stopped with a duration of: " + formattedResult());
    }
  }

  public String formattedResult() {
    if (state == State.STOPPED) {
      return getFormattedDuration();
    }

    return "Stopwatch has not been stopped yet.";
  }

  public String getFormattedDuration() {
    if (state == State.STOPPED) {
      formattedDuration = grvtime.millisToLongFormat(duration);
    }
    return formattedDuration;
  }

  @Override
  public String toString() {
    return formattedResult();
  }
}

