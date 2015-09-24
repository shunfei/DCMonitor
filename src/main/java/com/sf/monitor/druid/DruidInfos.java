package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Event;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.DCMZkUtils;
import com.sf.monitor.utils.JsonValues;
import com.sf.monitor.utils.PrometheusUtils;
import com.sf.monitor.utils.Utils;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DruidInfos {
  private static final Logger log = new Logger(DruidInfos.class);
  private static final String coordinatorLeaderElectionPath = "/coordinator/_COORDINATOR";
  private static final String overlordLeaderElectionPath = "/indexer/leaderLatchPath";

  public static class AnnounceNode {
    public String host;
    public String name;
    public String role;
    public String regTime;
    public String serviceType;

    public static List<Map<String, Object>> toMaps(List<AnnounceNode> nodes) {
      return Lists.transform(
        nodes, new Function<AnnounceNode, Map<String, Object>>() {
          @Override
          public Map<String, Object> apply(AnnounceNode node) {
            return ImmutableMap.of(
              "host",
              (Object) node.host,
              "name",
              node.name,
              "role",
              node.role,
              "regTime",
              node.regTime,
              "serviceType",
              node.serviceType
            );
          }
        }
      );
    }
  }

  @JsonProperty
  public String zkRootPath;
  @JsonProperty
  public String zkDiscoveryPath;
  @JsonProperty
  public String overlordName;
  @JsonProperty
  public String brokerName;
  @JsonProperty
  public String coordinatorName;
  @JsonProperty
  public String realtimeName;
  @JsonProperty
  public String historicalName;
  @JsonProperty
  public String middleManagerName;
  @JsonProperty
  public Map<String, Boolean> warningSpecs;

  private DruidService<DruidService.OverlordService> overlordService;
  private DruidService<DruidService.CoordinatorService> coordinatorService;

  public boolean shouldWarn(String metric) {
    if (warningSpecs == null) {
      return false;
    } else {
      Boolean b = warningSpecs.get(metric);
      return b == null ? false : b;
    }
  }

  public void init() {
    overlordService = new DruidService<DruidService.OverlordService>(
      zkRootPath + overlordLeaderElectionPath,
      DruidService.OverlordService.class
    );
    coordinatorService = new DruidService<DruidService.CoordinatorService>(
      zkRootPath + coordinatorLeaderElectionPath,
      DruidService.CoordinatorService.class
    );
  }

  private List<JsonValues> getDataServiceNodes(String type, String name) {
    DruidService.CoordinatorService service = coordinatorService.getService();
    if (service == null) {
      log.warn("coordinator service not found!");
      return Collections.emptyList();
    }
    List<Map<String, Object>> nodes = service.getDataServers();
    List<JsonValues> typeNodes = Lists.newArrayList();
    for (Map<String, Object> node : nodes) {
      if (type.equals(node.get("type"))) {
        node.put("used", (Double) node.get("currSize") / (Double) node.get("maxSize"));
        node.put("name", name);
        typeNodes.add(JsonValues.of(node, "name", "host", "type", "maxSize", "currSize", "used", "tier", "priority"));
      }
    }
    return typeNodes;
  }

  public List<JsonValues> getRealtimeNodes() {
    return getDataServiceNodes("realtime", realtimeName);
  }

  public List<JsonValues> getHistoricalNodes() {
    return getDataServiceNodes("historical", historicalName);
  }

  public List<JsonValues> getMiddleManagerNodes() {
    DruidService.OverlordService service = overlordService.getService();
    if (service == null) {
      log.warn("overlord service not found!");
      return Collections.emptyList();
    }
    return Lists.transform(
      service.getWorkers(), new Function<DruidService.MiddleManager, JsonValues>() {
        @Override
        public JsonValues apply(DruidService.MiddleManager input) {
          return input.toJsonValues();
        }
      }
    );
  }

  public List<AnnounceNode> getBrokerNodes() {
    return getAnnounceNodes(brokerName, null);
  }

  public List<AnnounceNode> getCoordinatorNodes() {
    return getAnnounceNodes(coordinatorName, zkRootPath + coordinatorLeaderElectionPath);
  }

  public List<AnnounceNode> getOverlordNodes() {
    return getAnnounceNodes(overlordName, zkRootPath + overlordLeaderElectionPath);
  }

  private List<AnnounceNode> getAnnounceNodes(String serviceName, String leaderElectionPath) {
    List<AnnounceNode> nodes = Lists.transform(
      DCMZkUtils.getZKChildrenContent(zkDiscoveryPath + "/" + serviceName, false),
      new Function<String, AnnounceNode>() {
        @Override
        public AnnounceNode apply(String input) {
          Map<String, Object> m = Utils.toMap(input);
          AnnounceNode node = new AnnounceNode();
          node.host = (String) m.get("address") + ":" + (Integer) m.get("port");
          node.name = (String) m.get("name");
          node.role = "-";
          node.regTime = new DateTime((Long) m.get("registrationTimeUTC")).toString();
          node.serviceType = (String) m.get("serviceType");
          return node;
        }
      }
    );
    String leaderHost = null;
    List<String> waitingHosts = Collections.emptyList();
    if (leaderElectionPath != null) {
      leaderHost = DCMZkUtils.getLeaderContent(leaderElectionPath);
      waitingHosts = DCMZkUtils.getZKChildrenContent(leaderElectionPath, false);
    }

    Map<String, AnnounceNode> nodeMap = Maps.newHashMap();
    for (AnnounceNode node : nodes) {
      if (node.host.equals(leaderHost)) {
        node.role = "leader";
      }
      nodeMap.put(node.host, node);
    }

    // Backup nodes won't announce themseleves until them got a chance to be leader,
    // we also need to put them in.
    for (String host : waitingHosts) {
      if (!nodeMap.containsKey(host)) {
        AnnounceNode node = new AnnounceNode();
        node.host = host;
        node.name = serviceName;
        node.regTime = "unknown";
        node.role = "-";
        node.serviceType = "unknown";
        nodeMap.put(host, node);
      }
    }
    return Lists.newArrayList(nodeMap.values());
  }

  public Map<String, List<Event>> getTrendData(Map<String, String> tags, DateTime from, DateTime to) {
    return PrometheusUtils.getEvents(EmitMetricsAnalyzer.tableName, tags, from, to);
  }

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();
    DruidInfos infos = Resources.druidInfos;
    ObjectMapper om = Resources.jsonMapper;

    System.out.println("realtime: " + om.writeValueAsString(infos.getRealtimeNodes()));
    System.out.println("broker: " + om.writeValueAsString(infos.getBrokerNodes()));
    System.out.println("historical: " + om.writeValueAsString(infos.getHistoricalNodes()));
    System.out.println("middle manager: " + om.writeValueAsString(infos.getMiddleManagerNodes()));
    System.out.println("coodinator: " + om.writeValueAsString(infos.getCoordinatorNodes()));
    System.out.println("overlord: " + om.writeValueAsString(infos.getOverlordNodes()));

    Resources.close();
  }

}
