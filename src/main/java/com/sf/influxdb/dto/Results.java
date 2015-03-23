package com.sf.influxdb.dto;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Results {
  public List<Result> results;
  public String err;

  public List<Series> getFirstResSeriesList() {
    Results.Result res = (results != null && results.size() > 0) ? results.get(0) : null;
    return res == null ? null : res.series;
  }

  public Series getFirstSeries(){
    List<Series> rows = getFirstResSeriesList();
    if (rows == null) {
      return null;
    }
    return getFirstResSeriesWith(Collections.<String, String>emptyMap());
  }

  public Series getFirstResSeriesWith(Map<String, String> tags) {
    List<Series> rows = getFirstResSeriesList();
    if (rows == null) {
      return null;
    }
    for (Series row : rows) {
      boolean match = true;
      for (Map.Entry<String, String> e : tags.entrySet()) {
        String tagName = e.getKey();
        Object tagValue = e.getValue();
        if (tagValue == null) {
          continue;
        }
        if (!tagValue.equals(row.tags.get(tagName))) {
          match = false;
          break;
        }
      }
      if (match) {
        return row;
      }
    }
    return null;
  }

  public static class Result {
    public List<Series> series;
    public String err;
  }
}
