package com.sf.influxdb.dto;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Series {
  public String name;
  public Map<String, Object> tags;
  public String[] columns;
  public Object[][] values;

  private List<Map<String, Object>> indexedValues;

  public List<Map<String, Object>> indexedValues() {
    if (indexedValues == null) {
      if (values == null){
        return Collections.emptyList();
      }
      indexedValues = Lists.newArrayListWithCapacity(values.length);
      for (Object[] r : values) {
        Map<String, Object> indexV = Maps.newLinkedHashMap();
        if (tags != null) {
          for (Map.Entry<String, Object> e : tags.entrySet()) {
            indexV.put(e.getKey(), e.getValue());
          }
        }
        for (int i = 0; i < columns.length; i++) {
          indexV.put(columns[i], r[i]);
        }
        indexedValues.add(indexV);
      }
    }
    return indexedValues;
  }
}
