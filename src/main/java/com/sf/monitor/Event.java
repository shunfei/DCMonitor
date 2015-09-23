package com.sf.monitor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.joda.time.DateTime;

import java.util.Map;

public class Event {
  @JsonIgnore
  public DateTime time;
  @JsonIgnore
  public Map<String, String> tags;
  public String timeStr;
  public String metricName;
  public Double metricValue;
}
