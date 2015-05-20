package com.sf.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.influxdb.dto.Point;

import java.util.List;

public abstract class CommonFetcher implements InfoFetcher {
  @JsonProperty
  public Boolean saveMetrics;

  public void saveMetrics(List<Point> points) {
    if (saveMetrics == null || saveMetrics){
      Resources.influxDB.write(
        Config.config.influxdb.influxdbDatabase,
        "",
        points
      );
    }
  }
}
