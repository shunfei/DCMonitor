package com.sf.monitor.druid;

import com.google.common.collect.Maps;
import com.sf.monitor.utils.JsonValues;

import java.util.List;
import java.util.Map;

public class MiddleManager {
  public WorkerHost worker;
  public String lastCompletedTaskTime;
  public int currCapacityUsed;
  public List<String> availabilityGroups;
  public List<String> runningTasks;

  public JsonValues toJsonValues() {
    Map<String, Object> map = Maps.newHashMap();
    map.put("host", worker.host);
    map.put("ip", worker.ip);
    map.put("capacity", worker.capacity);
    map.put("used", currCapacityUsed);
    map.put("version", worker.version);
    map.put("availabilityGroups", availabilityGroups);
    map.put("runningTasks", runningTasks);
    return new JsonValues(map);
  }

  public static class WorkerHost {
    public String host;
    public String ip;
    public int capacity;
    public String version;
  }
}


