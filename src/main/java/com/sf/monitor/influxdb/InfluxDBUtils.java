package com.sf.monitor.influxdb;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InfluxDBUtils {

  public static List<QueryResult.Series> getFirstResSeriesList(QueryResult result) {
    QueryResult.Result res = (result.getResults() != null && result.getResults().size() > 0)
                             ? result.getResults().get(0)
                             : null;
    return res == null ? null : res.getSeries();
  }

  public static QueryResult.Series getFirstSeries(QueryResult result) {
    List<QueryResult.Series> rows = getFirstResSeriesList(result);
    if (rows == null) {
      return null;
    }
    return getFirstResSeriesWith(result, Collections.<String, String>emptyMap());
  }

  public static QueryResult.Series getFirstResSeriesWith(QueryResult result, Map<String, String> tags) {
    List<QueryResult.Series> series = getFirstResSeriesList(result);
    if (series == null) {
      return null;
    }
    for (QueryResult.Series serie : series) {
      boolean match = true;
      for (Map.Entry<String, String> e : tags.entrySet()) {
        String tagName = e.getKey();
        Object tagValue = e.getValue();
        if (tagValue == null) {
          continue;
        }
        if (!tagValue.equals(serie.getTags().get(tagName))) {
          match = false;
          break;
        }
      }
      if (match) {
        return serie;
      }
    }
    return null;
  }

  public static List<List<Event>> commonQuery(String sql) {
    QueryResult result = Resources.influxDB.query(
      new Query(sql, Config.config.influxdb.influxdbDatabase)
    );
    final List<QueryResult.Series> seriesList = getFirstResSeriesList(result);
    if (seriesList == null) {
      return Collections.emptyList();
    }
    return Lists.transform(
      seriesList, new Function<QueryResult.Series, List<Event>>() {
        @Override
        public List<Event> apply(QueryResult.Series series) {
          return Event.fromSeries(series);
        }
      }
    );
  }

  public static List<Event> commonQueryFirstSeries(String sql) {
    QueryResult result = Resources.influxDB.query(
      new Query(sql, Config.config.influxdb.influxdbDatabase)
    );
    QueryResult.Series series = getFirstSeries(result);
    if (series == null) {
      return Collections.emptyList();
    }
    return Event.fromSeries(series);
  }

  public static void saveEvents(List<Event> events) {
    for (Event e : events) {
      Resources.influxDB.write(Config.config.influxdb.influxdbDatabase, "", e.toPoint());
    }
  }
}
