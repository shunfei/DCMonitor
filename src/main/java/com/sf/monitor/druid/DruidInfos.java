package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Series;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.DCMZkUtils;
import com.sf.monitor.utils.JsonValues;
import com.sf.monitor.utils.TagValue;
import com.sf.monitor.utils.Utils;
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

  public static class MetricsParam {
    public String from;
    public String to;
    public List<String> metrics;
    public List<TagValue> tagValues;
    public List<String> groups;
    public boolean debug;
    public Integer limit;

    @JsonIgnore
    public DateTime fromDateTime;
    @JsonIgnore
    public DateTime toDateTime;
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

  public static class TaggedValues {
    public Map<String, Object> tags;
    public List<JsonValues> values;
  }

  public Result<List<TaggedValues>> getTrendData(MetricsParam param) {
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
    String groupby;
    if (param.groups == null || param.groups.size() == 0) {
      groupby = "";
    } else {
      groupby = " group by " + Joiner.on(",").join(param.groups);
    }
    String limit;
    if (param.limit == null) {
      limit = "";
    } else {
      limit = " limit " + param.limit;
    }

    String sql = String.format(
      "select %s from %s where %s%s%s",
      selects,
      EmitMetricsAnalyzer.tableName,
      where,
      groupby,
      limit
    );

    log.debug(sql);

    Results results = Resources.influxDB.query(
      Config.config.influxdb.influxdbDatabase,
      sql
    );
    Object debugMsg = param.debug ? sql : null;
    final List<TaggedValues> taggedValuesList;

    Series series = results.getFirstSeries();
    final List<Series> seriesList = results.getFirstResSeriesList();
    if (seriesList == null) {
      taggedValuesList = Collections.emptyList();
    } else {
      taggedValuesList = Lists.transform(
        seriesList, new Function<Series, TaggedValues>() {
          @Override
          public TaggedValues apply(Series series) {
            TaggedValues taggedValues = new TaggedValues();
            taggedValues.tags = series.tags;
            taggedValues.values = Lists.transform(
              series.indexedValues(),
              new Function<Map<String, Object>, JsonValues>() {
                @Override
                public JsonValues apply(Map<String, Object> row) {
                  return JsonValues.of(row);
                }
              }
            );
            return taggedValues;
          }
        }
      );
    }
    return new Result<List<TaggedValues>>(taggedValuesList, true, debugMsg);
  }

  public Result<List<TaggedValues>> getLatestData(MetricsParam param) {
    DateTime now = new DateTime();
    param.fromDateTime = now.minusMinutes(5);
    param.toDateTime = now;
    param.limit = 1;
    return getTrendData(param);
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
