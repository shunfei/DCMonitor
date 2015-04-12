package com.sf.monitor.druid;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Point;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmitMetricsAnalyzer {
  private static final Logger log = new Logger(EmitMetricsAnalyzer.class);
  public static final String tableName = "druid_metrics";

  public static class MetricInfo {
    @JsonProperty
    public String feed;
    @JsonProperty
    public String timestamp;
    @JsonProperty
    public String service;
    @JsonProperty
    public String host;
    @JsonProperty
    public String metric;
    @JsonProperty
    public double value;
    @JsonProperty
    public String severity;
    @JsonProperty
    public String description;
    @JsonProperty
    public String user1;
    @JsonProperty
    public String user2;
    @JsonProperty
    public String user3;
    @JsonProperty
    public String user4;
    @JsonProperty
    public Object user5;
    @JsonProperty
    public Object user6;
    @JsonProperty
    public Object user7;
    @JsonProperty
    public Object user8;
    @JsonProperty
    public Object user9;
    @JsonProperty
    public Object user10;
  }

  public static List<Point> fetchReceivedInfos(List<MetricInfo> rawInfos) {
    if (rawInfos == null || rawInfos.size() == 0) {
      return Collections.emptyList();
    }
    String timestamp = rawInfos.get(0).timestamp;
    List<Point> points = Lists.newArrayList();
    for (MetricInfo info : rawInfos) {
      for (MetricFetchers fetchers : allMetrics) {
        Point p = fetchers.toPoint(info);
        if (p != null) {
          points.add(p);
        }
      }
    }
    return Utils.mergePoints(points, tableName, timestamp);

  }

  private static MetricFetchers systemMetrics = new MetricFetchers(
    // System metrics
    new MetricFetcher("sys/mem").withKeys("max", "used").withUsers("user1", "type"),
    new MetricFetcher("sys/fs").withKeys("max", "used").withUsers("user1", "device", "user2", "mount"),
    new MetricFetcher("sys/disk").withKeys("write/count", "write/size", "read/count", "read/size")
                                 .withUsers("user1", "device", "user2", "mount"),
    new MetricFetcher("sys/net").withKeys("write/size", "read/size")
                                .withUsers("user1", "device", "user2", "ip", "user3", "ether"),
    new MetricFetcher("sys/cpu").withUsers("user1", "cpuid", "user2", "type"),
    new MetricFetcher("sys/swap").withKeys("max", "free", "pageIn", "pageOut")
  );
  private static MetricFetchers jvmMetrics = new MetricFetchers(
    new MetricFetcher("jvm/mem").withKeys("committed", "init", "max", "used").withUsers("user1", "type"),
    new MetricFetcher("jvm/pool").withKeys("committed", "init", "max", "used")
                                 .withUsers("user1", "type", "user2", "part"),
    new MetricFetcher("jvm/gc").withKeys("time", "count").withUsers("user1", "type"),
    new MetricFetcher("jvm/bufferpool").withKeys("capacity", "used").withUsers("user1", "type")
  );
  private static MetricFetchers cacheMetrics = new MetricFetchers(
    new MetricFetcher("cache/delta").withKeys(
      "numEntries",
      "numEntries",
      "sizeBytes",
      "hits",
      "misses",
      "evictions",
      "averageBytes",
      "timeouts",
      "errors",
      "hitRate"
    )
  );
  private static MetricFetchers segmentMetrics = new MetricFetchers(
    new MetricFetcher("server/segment").withKeys("used", "usedPercent", "count").withUsers(
      "user1",
      "dataSource",
      "user2",
      "tier"
    ),
    new MetricFetcher("server/segment").withKeys("max"),
    new MetricFetcher("server/segment").withKeys("totalUsed", "totalCount", "totalUsedPercent")
                                       .withUsers("user2", "tier")
                                       .withWarning(
                                         "server/segment/totalUsedPercent", new Warning(
                                           0.85,
                                           //0,
                                           "running out of segment space"
                                         )
                                       )
  );

  private static MetricFetchers indexMetrics = new MetricFetchers(
    new MetricFetcher("events").withKeys("thrownAway", "unparseable", "processed").withUsers("user2", "dataSource"),
    new MetricFetcher("rows/output").withUsers("user2", "dataSource"),
    new MetricFetcher("persists").withKeys("num", "time", "backPressure").withUsers("user2", "dataSource")
  );

  // Those are the metrics we care about currently.
  private static MetricFetchers[] allMetrics = new MetricFetchers[]{
    systemMetrics,
    jvmMetrics,
    cacheMetrics,
    segmentMetrics,
    indexMetrics
  };

  private static class MetricFetcher {
    final String name;
    List<String> accpetMetrics;
    Map<String, String> userMap = Maps.newHashMap();
    Map<String, Warning> warnings = Maps.newHashMap();

    MetricFetcher(String name) {
      this.name = StringUtils.removeStart(StringUtils.removeEnd(name, "/"), "/");
      this.accpetMetrics = Collections.singletonList(name);
    }

    MetricFetcher withKeys(String... keys) {
      this.accpetMetrics = Lists.transform(
        Lists.newArrayList(keys), new Function<String, String>() {
          @Override
          public String apply(String key) {
            return name + "/" + StringUtils.removeStart(StringUtils.removeEnd(key, "/"), "/");
          }
        }
      );
      return this;
    }

    MetricFetcher withUsers(String... users) {
      for (int i = 0; i + 1 < users.length; i += 2) {
        userMap.put(users[i], users[i + 1]);
      }
      return this;
    }

    MetricFetcher withWarning(String key, Warning warning) {
      warnings.put(key, warning);
      return this;
    }

    Iterable<String> accpetMetrics() {
      return accpetMetrics;
    }

    Point toPoint(MetricInfo info) {
      Point p = new Point();
      String colKey = Utils.smoothText.apply(info.metric);
      p.fields = ImmutableMap.of(colKey, (Object) info.value);
      Warning warning = warnings.get(info.metric);
      p.tags = Maps.newLinkedHashMap(); // We need the order!
      p.tags.put("service", info.service);
      p.tags.put("host", info.host);
      for (Map.Entry<String, String> e : userMap.entrySet()) {
        String user = e.getKey();
        String dim = e.getValue();
        if ("user1".equals(user)) {
          p.tags.put(dim, info.user1);
        } else if ("user2".equals(user)) {
          p.tags.put(dim, info.user2);
        } else if ("user3".equals(user)) {
          p.tags.put(dim, info.user3);
        } else if ("user4".equals(user)) {
          p.tags.put(dim, info.user4);
        }
      }

      if (warning != null) {
        warning.checkAlarm(info, p.tags);
      }

      return p;
    }
  }

  private static class Warning {
    static enum Watcher {
      ShouldSmaller,
      ShouldBigger
    }

    double threshold;
    String warning;
    Watcher watcher = Watcher.ShouldSmaller;

    Warning(double threshold, String warning) {
      this.threshold = threshold;
      this.warning = warning;
    }

    Warning withWatcher(Watcher watcher) {
      this.watcher = watcher;
      return this;
    }

    void checkAlarm(MetricInfo info, Map<String, String> tags) {
      boolean shouldWarn = false;
      if (watcher == Watcher.ShouldBigger) {
        shouldWarn = info.value < threshold;
      } else if (watcher == Watcher.ShouldSmaller) {
        shouldWarn = info.value > threshold;
      }
      if (shouldWarn) {
        String warnMsg = getWarning(info, tags);
        Utils.sendNotify("druid", warnMsg);
        log.warn("druid - " + warnMsg);
      }
    }

    String getWarning(MetricInfo info, Map<String, String> tags) {
      String tagStr = Joiner.on(',').join(
        Iterables.transform(
          tags.entrySet(), new Function<Map.Entry<String, String>, String>() {

            @Override
            public String apply(Map.Entry<String, String> e) {
              return e.getKey() + ":[" + e.getValue() + "]";
            }
          }
        )
      );
      return String.format(
        "%s - %s: current[%f], threshold[%f], %s",
        tagStr,
        info.metric,
        info.value,
        threshold,
        warning
      );
    }
  }

  private static class MetricFetchers {
    final Map<String, MetricFetcher> fetchers;

    MetricFetchers(MetricFetcher... mappers) {
      this.fetchers = Maps.newHashMap();
      for (MetricFetcher fetcher : mappers) {
        for (String metric : fetcher.accpetMetrics()) {
          fetchers.put(metric, fetcher);
        }
      }
    }

    Point toPoint(MetricInfo info) {
      if (!"metrics".equals(info.feed)) {
        return null;
      }
      MetricFetcher fetcher = fetchers.get(info.metric);
      if (fetcher == null) {
        return null;
      }
      return fetcher.toPoint(info);
    }
  }

  private static List<MetricInfo> parseMetrics(String rawMetricStr) {
    try {
      return Resources.jsonMapper.readValue(
        rawMetricStr, new TypeReference<List<MetricInfo>>() {
        }
      );
    } catch (Exception e) {
      log.error(e, "failed to parse metric infos");
      return Collections.emptyList();
    }
  }

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();

    String msg = IOUtils.toString(ClassLoader.getSystemResourceAsStream("druid_metrics_msg"));

    List<Point> points = EmitMetricsAnalyzer.fetchReceivedInfos(parseMetrics(msg));

    Resources.influxDB.write("dcmonitor", "", points);

    System.out.println(Resources.jsonMapper.writeValueAsString(points));
  }

}
