package com.sf.influxdb.dto;

import java.util.List;

public class Write {
  public String database;
  public String retentionPolicy;
  public List<Point> points;

  public Write(String database, String retentionPolicy, List<Point> points) {
    this.database = database;
    this.retentionPolicy = retentionPolicy;
    this.points = points;
  }
}
