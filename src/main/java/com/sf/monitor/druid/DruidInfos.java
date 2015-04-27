package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Series;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.JsonValues;
import com.sf.monitor.utils.TagValue;
import com.sf.monitor.utils.Utils;
import com.sf.monitor.utils.DCMZkUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
  }

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

  private DruidService<DruidService.OverlordService> overlordService;
  private DruidService<DruidService.CoordinatorService> coordinatorService;

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

  private List<JsonValues> getDataServiceNodes(String type) {
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
        typeNodes.add(JsonValues.of(node, "type", "host", "maxSize", "currSize", "used", "tier", "priority"));
      }
    }
    return typeNodes;
  }

  public List<JsonValues> getRealtimeNodes() {
    return getDataServiceNodes("realtime");
  }

  public List<JsonValues> getHistoricalNodes() {
    return getDataServiceNodes("historical");
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
    return getAnnounceNodes(zkBrokerName, null);
  }

  public List<AnnounceNode> getCoordinatorNodes() {
    return getAnnounceNodes(zkCoordinatorName, zkRootPath + coordinatorLeaderElectionPath);
  }

  public List<AnnounceNode> getOverlordNodes() {
    return getAnnounceNodes(zkOverlordName, zkRootPath + overlordLeaderElectionPath);
  }

  private List<AnnounceNode> getAnnounceNodes(String serviceName, String leaderElectionPath) {
    List<AnnounceNode> nodes = Lists.transform(
      DCMZkUtils.getZKChildrenContent(zkDiscoveryPath + "/" + serviceName, false), new Function<String, AnnounceNode>() {
        @Override
        public AnnounceNode apply(String input) {
          Map<String, Object> m = Utils.toMap(input);
          AnnounceNode node = new AnnounceNode();
          node.host = (String) m.get("address") + ":" + (Integer) m.get("port");
          node.name = (String) m.get("topoName");
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

  public static class MetricsParam {
    public String from;
    public String to;
    public DateTime fromDateTime;
    public DateTime toDateTime;
    public List<String> metrics;
    public List<TagValue> tagValues;
    public boolean debug;
  }

  public static class Result<T> {
    public T res;
    public boolean suc;
    public Object debugMsg;

    public Result(T res, boolean suc, Object debugMsg) {
      this.res = res;
      this.suc = suc;
      this.debugMsg = debugMsg;
    }
  }

  public Result<List<JsonValues>> getTrendData(MetricsParam param) {
    List<TagValue> nullCheck = Lists.transform(
      param.metrics, new Function<String, TagValue>() {
        @Override
        public TagValue apply(String input) {
          return new TagValue(input, TagValue.GreaterEqual, 0);
        }
      }
    );
    String fromStr = param.fromDateTime.withZone(DateTimeZone.UTC).toString();
    String toStr = param.toDateTime.withZone(DateTimeZone.UTC).toString();
    List<TagValue> timeLimit = ImmutableList.of(
      new TagValue("time", TagValue.GreaterEqual, fromStr),
      new TagValue("time", TagValue.LessEqaul, toStr)
    );
    String selects = Joiner.on(",").join(param.metrics);
    String where = Joiner.on(" and ").join(
      Iterators.transform(
        Iterators.concat(param.tagValues.iterator(), nullCheck.iterator(), timeLimit.iterator()),
        new Function<TagValue, String>() {
          @Override
          public String apply(TagValue input) {
            return "(" + input.toSql() + ")";
          }
        }
      )
    );

    String sql = String.format(
      "select %s from %s where %s ",
      selects,
      EmitMetricsAnalyzer.tableName,
      where
    );

    System.out.println(sql);

    Results results = Resources.influxDB.query(
      Config.config.influxdb.influxdbDatabase,
      sql
    );
    Series series = results.getFirstSeries();

    Object debugMsg = param.debug ? sql : null;
    List<JsonValues> res;
    if (series == null) {
      res = Collections.emptyList();
    } else {
      res = Lists.transform(
        series.indexedValues(), new Function<Map<String, Object>, JsonValues>() {
          @Override
          public JsonValues apply(Map<String, Object> row) {
            return JsonValues.of(row);
          }
        }
      );
    }
    return new Result<List<JsonValues>>(res, true, debugMsg);
  }

  public Result<JsonValues> getLatestData(MetricsParam param) {
    DateTime now = new DateTime();
    param.fromDateTime = now.minusMinutes(5);
    param.toDateTime = now;
    Result<List<JsonValues>> result = getTrendData(param);
    if (result.res.size() > 0) {
      return new Result<JsonValues>(result.res.get(0), true, result.debugMsg);
    } else {
      return new Result<JsonValues>(null, true, result.debugMsg);
    }
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

    MetricsParam param = new MetricsParam();
    param.toDateTime = new DateTime();
    param.fromDateTime = param.toDateTime.minusMinutes(10);
    param.metrics = ImmutableList.of(
      "events_processed",
      "events_thrownAway",
      "events_unparseable"
    );
    param.tagValues = ImmutableList.of(
      new TagValue(
        "host",
        TagValue.In,
        ImmutableList.of(
          "192.168.10.51:8001",
          "192.168.10.52:8001"
        )
      )
    );

    System.out.println("trendData: " + om.writeValueAsString(infos.getTrendData(param)));
    System.out.println("latestData: " + om.writeValueAsString(infos.getLatestData(param)));

    Resources.close();
  }

}
