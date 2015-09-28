package com.sf.monitor.utils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Event;
import com.sf.monitor.Resources;
import com.sf.prometheus.PrometheusQueryService;
import io.prometheus.client.metrics.Gauge;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PrometheusUtils {
  private static final Logger log = new Logger(PrometheusUtils.class);
  private static final Map<String, Gauge> gauges = new HashMap<>();

  private static Gauge getGauge(String name) {
    Gauge gauge = gauges.get(name);
    if (gauge != null) {
      return gauge;
    }
    synchronized (gauges) {
      gauge = Gauge.newBuilder().namespace(Config.config.prometheus.namespace).name(name).documentation(
        String.format(
          "mectric for %s model",
          name
        )
      ).build();
      gauges.put(name, gauge);
    }
    return gauge;
  }

  public static void saveEvents(String modelName, List<Event> events) {
    try {
      if (log.isDebugEnabled()) {
        log.debug("saveEvents, modelName: %s, events: %s", modelName, Resources.jsonMapper.writeValueAsString(events));
      }
    } catch (Exception e) {
    }
    Gauge gauge = getGauge(modelName);
    for (Event event : events) {
      Gauge.Partial p = gauge.newPartial();
      for (Map.Entry<String, String> e : event.tags.entrySet()) {
        p.labelPair(e.getKey(), e.getValue());
      }
      p.labelPair("metricName", event.metricName);
      p.apply().set(event.metricValue);
    }
  }

  public static Map<String, List<Event>> getEvents(
    String modelName,
    Map<String, String> tags,
    DateTime start,
    DateTime end
  ) {
    String query = Config.config.prometheus.namespace + "_" + modelName;
    query += "{";
    query += Joiner.on(",").join(
      Iterables.transform(
        tags.entrySet(), new Function<Map.Entry<String, String>, String>() {
          @Nullable
          @Override
          public String apply(Map.Entry<String, String> e) {
            return String.format("%s=\"%s\"", e.getKey(), e.getValue());
          }
        }
      )
    );
    query += "}";
    PrometheusQueryService.Result res = Resources.prometheusQuery.query(query, start.getMillis(), end.getMillis());
    try {
      log.debug("getEvents, query: %s, res: %s", query, Resources.jsonMapper.writeValueAsString(res));
    } catch (Exception e) {
    }
    if (!"success".equals(res.status)) {
      log.error("Prometheus query[%s] errorType: %s, error: %s", query, res.errorType, res.error);
      return Collections.emptyMap();
    }
    Map<String, List<Event>> mappedEvents = new HashMap<>();
    for (PrometheusQueryService.MetricItem item : res.data.result) {
      String metricName = item.metric.get("metricName");
      if (StringUtils.isEmpty(metricName)) {
        continue;
      }
      List<Event> events = mappedEvents.get(metricName);
      if (events == null) {
        events = new ArrayList<>();
        mappedEvents.put(metricName, events);
      }
      for (Object[] vs : item.values) {
        Event event = new Event();
        event.metricName = metricName;
        event.timeStamp = (long) ((double) vs[0]) * 1000;
        event.metricValue = Double.valueOf((String) vs[1]);
        events.add(event);
      }
    }
    return mappedEvents;
  }
}
