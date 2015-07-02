package com.sf.monitor.influxdb;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Event {
  public String name;
  public DateTime timestamp;
  public Map<String, String> tags;
  public Map<String, Object> values;

  public Point toPoint(){
    DateTime timestamp = (this.timestamp != null) ? this.timestamp : DateTime.now();
    Point.Builder b = Point.measurement(name).time(timestamp.getMillis(), TimeUnit.MILLISECONDS);
    if (tags != null) {
      for (Map.Entry<String, String> e : tags.entrySet()){
        b.tag(e.getKey(), e.getValue());
      }
    }
    for(Map.Entry<String, Object> e : values.entrySet()) {
      b.field(e.getKey(), e.getValue());
    }
    return b.build();
  }

  public static List<Event> fromSeries(final QueryResult.Series series) {
    if (series.getValues() == null) {
      return Collections.emptyList();
    }
    return Lists.transform(
      series.getValues(), new Function<List<Object>, Event>() {
        @Override
        public Event apply(List<Object> value) {
          Event e = new Event();
          e.name = series.getName();
          e.tags = series.getTags();
          e.values = Maps.newHashMap();
          for (int i = 0; i < series.getColumns().size(); i++) {
            String colName = series.getColumns().get(i);
            e.values.put(colName, value.get(i));
            if ("time".equals(colName)){
              e.timestamp = DateTime.parse((String)(value.get(i)));
            }
          }
          return e;
        }
      }
    );
  }


  public static List<Event> mergePoints(Iterable<Event> events, String name, DateTime timestamp) {
    Map<String, Event> eventMap = Maps.newHashMap();
    for (Event p : events) {
      String key = Joiner.on("-").join(
        Iterables.transform(
          p.tags.entrySet(), new Function<Map.Entry<String, String>, String>() {
            @Override
            public String apply(Map.Entry<String, String> input) {
              return input.getKey() + ":" + input.getValue();
            }
          }
        )
      );
      Event oldEvent = eventMap.get(key);
      if (oldEvent == null) {
        Event newEvent = new Event();
        newEvent.name = name;
        newEvent.timestamp = timestamp;
        newEvent.tags = Maps.newHashMap(p.tags);
        newEvent.values = Maps.newHashMap(p.values);
        eventMap.put(key, newEvent);
      } else {
        for (Map.Entry<String, Object> e : p.values.entrySet()) {
          oldEvent.values.put(e.getKey(), e.getValue());
        }
        eventMap.put(key, oldEvent);
      }
    }
    return Lists.newArrayList(eventMap.values());
  }

}
