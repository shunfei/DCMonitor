package com.sf.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.monitor.influxdb.Event;
import com.sf.monitor.influxdb.InfluxDBUtils;

import java.util.List;

public abstract class CommonFetcher implements InfoFetcher {
  @JsonProperty
  public Boolean saveMetrics;

  public void saveMetrics(List<Event> events) {
    if (saveMetrics == null || saveMetrics){
      InfluxDBUtils.saveEvents(events);
    }
  }
}
