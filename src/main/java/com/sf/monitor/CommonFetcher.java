package com.sf.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sf.monitor.kafka.KafkaStats;
import com.sf.monitor.utils.PrometheusUtils;

import java.util.List;

public abstract class CommonFetcher implements InfoFetcher {
  @JsonProperty
  public Boolean saveMetrics;

  public void saveMetrics(List<Event> events) {
    if (saveMetrics == null || saveMetrics) {
      PrometheusUtils.saveEvents(KafkaStats.tableName, events);
    }
  }
}
