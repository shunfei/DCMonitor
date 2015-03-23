package com.sf.influxdb.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class Point {
  @JsonProperty
  public String name;
  @JsonProperty
  public Map<String, String> tags;
  @JsonProperty
  public String timestamp;
  @JsonProperty
  public Map<String, Object> fields;
}