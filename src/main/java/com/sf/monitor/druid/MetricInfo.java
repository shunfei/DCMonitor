package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricInfo {
  @JsonProperty
  public String feed;
  @JsonProperty
  public String timestamp;
  @JsonProperty
  public String service;
  @JsonProperty
  public String host;
  @JsonProperty
  public String metric;
  @JsonProperty
  public double value;
  @JsonProperty
  public String severity;
  @JsonProperty
  public String description;
  @JsonProperty
  public String user1;
  @JsonProperty
  public String user2;
  @JsonProperty
  public String user3;
  @JsonProperty
  public String user4;
  @JsonProperty
  public Object user5;
  @JsonProperty
  public Object user6;
  @JsonProperty
  public Object user7;
  @JsonProperty
  public Object user8;
  @JsonProperty
  public Object user9;
  @JsonProperty
  public Object user10;
}