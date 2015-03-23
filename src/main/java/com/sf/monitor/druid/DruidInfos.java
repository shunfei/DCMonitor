package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Series;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.HttpRequest;
import com.sf.monitor.utils.JsonValues;
import com.sf.monitor.utils.Utils;
import com.sf.monitor.utils.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DruidInfos {
  private static final Logger log = new Logger(DruidInfos.class);
  private static final String dataServingHostAnnPath = "/announcements";
  private static final String coordinatorHostPath = "/coordinator/_COORDINATOR";
  private static final String overlordHostPath = "/indexer/leaderLatchPath";
  private static final String middleManagerHostPath = "/indexer/announcements";

  @JsonProperty
  public boolean zkInfoCompress;
  @JsonProperty
  public String zkRootPath;
  @JsonProperty
  public String zkDiscoveryPath;
  @JsonProperty
  public String zkOverlordName;
  @JsonProperty
  public String zkBrokerName;
  @JsonProperty
  public String zkCoordinatorName;

  private DruidOverlordService overlordService;

  public void init() {
    DruidNode masterOverlord = getMasterOverlord();
    if (masterOverlord != null) {
      String overlordUrl = String.format("http://%s:%d", masterOverlord.address, masterOverlord.port);
      overlordService = HttpRequest.create(overlordUrl, DruidOverlordService.class);
    }
  }

  public List<JsonValues> getBrokers() {
    List<String> brokers = ZkUtils.getZKChildContentOf(zkDiscoveryPath + '/' + zkBrokerName, false);
    return Lists.transform(
      brokers, new Function<String, JsonValues>() {
        @Override
        public JsonValues apply(String input) {
          return JsonValues.of(Utils.toMap(input), "host", "name", "regTime", "serviceType");
        }
      }
    );
  }

  public List<JsonValues> getRealtimes() {
    List<Map<String, Object>> nodes = ZkUtils.getZKChildContentAsMap(
      zkRootPath + dataServingHostAnnPath,
      zkInfoCompress
    );
    List<JsonValues> realtimeNodes = Lists.newArrayList();
    for (Map<String, Object> node : nodes) {
      if ("realtime".equals(node.get("type"))) {
        realtimeNodes.add(JsonValues.of(node, "type", "host", "maxSize", "tier", "priority"));
      }
    }
    return realtimeNodes;
  }

  public List<JsonValues> getHistoricals() {
    List<Map<String, Object>> nodes = ZkUtils.getZKChildContentAsMap(
      zkRootPath + dataServingHostAnnPath,
      zkInfoCompress
    );
    Results results = Resources.influxDB.query(
      Config.config.influxdb.influxdbDatabase,
      "select last(totalUsedPercent) as totalUsedPercent from druid_server_segment group by host"
    );

    List<JsonValues> realtimeNodes = Lists.newArrayList();
    for (Map<String, Object> node : nodes) {
      if (!"historical".equals(node.get("type"))) {
        continue;
      }
      Series row = results.getFirstResSeriesWith(ImmutableMap.of("host", (String) node.get("host")));
      if (row != null) {
        node.put("used", row.indexedValues().get(0).get("totalUsedPercent"));
      }

      realtimeNodes.add(JsonValues.of(node, "type", "host", "maxSize", "used", "tier", "priority"));
    }
    return realtimeNodes;
  }

  public List<JsonValues> getMiddleManagers() {
    if (overlordService == null) {
      return Collections.emptyList();
    }
    return Lists.transform(
      overlordService.getWorkers(), new Function<MiddleManager, JsonValues>() {
        @Override
        public JsonValues apply(MiddleManager input) {
          return input.toJsonValues();
        }
      }
    );
  }

  public List<DruidNode> getCoordinators() {
    return getNodes(zkRootPath + coordinatorHostPath, zkDiscoveryPath + '/' + zkCoordinatorName);
  }

  public List<DruidNode> getOverlords() {
    return getNodes(zkRootPath + overlordHostPath, zkDiscoveryPath + '/' + zkOverlordName);
  }


  private List<DruidNode> getNodes(String nodePath, String masterPath) {
    List<String> hosts = ZkUtils.getZKChildContentOf(nodePath, true);
    DruidNode masterNode = getMasterNode(masterPath);
    if (masterNode == null) {
      return Collections.emptyList();
    }
    String masterHost = masterNode.address + ":" + masterNode.port;
    List<DruidNode> nodes = Lists.newArrayList();
    nodes.add(masterNode);
    for (String host : hosts) {
      if (!StringUtils.equals(host, masterHost)) {
        DruidNode node = new DruidNode();
        node.address = host.split(":")[0];
        node.port = Integer.parseInt(host.split(":")[1]);
        node.name = "unknown";
        node.role = "backup";
        node.regTime = "unknown";
        node.serviceType = "unknown";
        nodes.add(node);
      }
    }
    return nodes;
  }

  private DruidNode getMasterNode(String masterPath) {
    List<String> masterStr = ZkUtils.getZKChildContentOf(masterPath, false);
    if (masterStr == null || masterStr.size() == 0) {
      return null;
    }
    DruidNode node = Utils.toObject(masterStr.get(0), DruidNode.class);
    node.regTime = new DateTime(node.registrationTimeUTC).toString();
    node.role = "master";
    return node;
  }

  public DruidNode getMasterOverlord() {
    return getMasterNode(zkDiscoveryPath + '/' + zkOverlordName);
  }

  public DruidNode getMasterCoodinator() {
    return getMasterNode(zkDiscoveryPath + '/' + zkCoordinatorName);
  }

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();
    DruidInfos infos = Resources.druidInfos;
    ObjectMapper om = Resources.jsonMapper;

    System.out.println("realtime: " + om.writeValueAsString(infos.getRealtimes()));
    System.out.println("broker: " + om.writeValueAsString(infos.getBrokers()));
    System.out.println("historical: " + om.writeValueAsString(infos.getHistoricals()));
    System.out.println("middle manager: " + om.writeValueAsString(infos.getMiddleManagers()));
    System.out.println("coodinator: " + om.writeValueAsString(infos.getCoordinators()));
    System.out.println("overlord: " + om.writeValueAsString(infos.getOverlords()));

    Resources.close();
  }

}
