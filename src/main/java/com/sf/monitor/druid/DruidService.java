package com.sf.monitor.druid;

import com.google.common.collect.Maps;
import com.sf.monitor.utils.HttpRequest;
import com.sf.monitor.utils.JsonValues;
import com.sf.monitor.utils.ZkUtils;
import org.apache.commons.lang.StringUtils;
import retrofit.http.GET;

import java.util.List;
import java.util.Map;

public class DruidService<T> {

  private final String leaderElectionPath;
  private final Class<T> serviceClass;

  private String leaderHost;
  private T leaderService;

  public DruidService(String leaderElectionPath, Class<T> serviceClass) {
    this.leaderElectionPath = leaderElectionPath;
    this.serviceClass = serviceClass;
  }

  public T getService() {
    String host = ZkUtils.getLeaderContent(leaderElectionPath);
    if (StringUtils.isEmpty(host)) {
      return null;
    }
    if (!host.equals(leaderHost) || leaderService == null) {
      leaderHost = host;
      String url = String.format("http://%s", leaderHost);
      leaderService = HttpRequest.create(url, serviceClass);
    }
    return leaderService;
  }

  public interface OverlordService {
    @GET("/druid/indexer/v1/workers")
    public List<MiddleManager> getWorkers();
  }

  public interface CoordinatorService {
    @GET("/druid/coordinator/v1/servers?simple")
    public List<Map<String, Object>> getDataServers();

  }

  public static class MiddleManager {
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


}
