package com.sf.monitor.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.influxdb.Event;
import com.sf.monitor.utils.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmitMetricsAnalyzer {
  private static final Logger log = new Logger(EmitMetricsAnalyzer.class);
  public static final String tableName = "druid_metrics";

  public static class MetricInfo {
    public String feed;
    public String timestamp;
    public String service;
    public String host;
    public String metric;
    public double value;
    public String severity;
    public String description;
    public String user1;
    public String user2;
    public String user3;
    public String user4;
    public Object user5;
    public Object user6;
    public Object user7;
    public Object user8;
    public Object user9;
    public Object user10;
  }

  public static List<Event> fetchReceivedInfos(List<MetricInfo> rawInfos) {
    if (rawInfos == null || rawInfos.size() == 0) {
      return Collections.emptyList();
    }
    String timestamp = rawInfos.get(0).timestamp;
    List<Event> points = Lists.newArrayList();
    for (MetricInfo info : rawInfos) {
      for (MetricFetchers fetchers : allMetrics) {
        Event p = fetchers.toPoint(info);
        if (p != null) {
          points.add(p);
        }
      }
    }

    return Event.mergePoints(points, tableName, DateTime.parse(timestamp));
  }

  private static MetricFetchers systemMetrics = new MetricFetchers(
    new MetricFetcher("sys/mem")
      .withKeys("max", "used"),
    new MetricFetcher("sys/fs")
      .withKeys("max", "used")
      .withUsers(
        "user1", "device",
        "user2", "mount"
      ),
    new MetricFetcher("sys/disk")
      .withKeys("write/count", "write/size", "read/count", "read/size")
      .withUsers(
        "user1", "device",
        "user2", "mount"
      ),
    new MetricFetcher("sys/net")
      .withKeys("write/size", "read/size")
      .withUsers(
        "user1", "device",
        "user2", "ip",
        "user3", "ether"
      ),
    new MetricFetcher("sys/cpu")
      .withUsers(
        "user1", "cpuid",
        "user2", "type"
      ),
    new MetricFetcher("sys/swap")
      .withKeys("max", "free", "pageIn", "pageOut")
  );

  private static MetricFetchers jvmMetrics = new MetricFetchers(
    new MetricFetcher("jvm/mem")
      .withKeys("committed", "init", "max", "used")
      .withUsers("user1", "type"),
    new MetricFetcher("jvm/pool")
      .withKeys("committed", "init", "max", "used")
      .withUsers(
        "user1", "type",
        "user2", "space"
      ),
    new MetricFetcher("jvm/gc")
      .withKeys("time", "count")
      .withUsers("user1", "type"),
    new MetricFetcher("jvm/bufferpool")
      .withKeys("capacity", "used")
      .withUsers("user1", "type")
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
    new MetricFetcher("server/segment")
      .withKeys("used", "usedPercent", "count")
      .withUsers(
        "user1", "dataSource",
        "user2", "tier"
      ),
    new MetricFetcher("server/segment")
      .withKeys("max"),
    new MetricFetcher("server/segment")
      .withKeys("totalUsed", "totalCount", "totalUsedPercent")
      .withUsers("user2", "tier")
      .withWarning("server/segment/totalUsedPercent", new Warning(0.85, "running out of segment space"))
  );

  private static MetricFetchers indexMetrics = new MetricFetchers(
    new MetricFetcher("events")
      .withKeys("thrownAway", "unparseable", "processed")
      .withUsers("user2", "dataSource"),
    new MetricFetcher("rows/output")
      .withUsers("user2", "dataSource"),
    new MetricFetcher("persists")
      .withKeys("num", "time", "backPressure")
      .withUsers("user2", "dataSource")
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

    Event toPoint(MetricInfo info) {
      Event p = new Event();
      String metricName = Utils.smoothText.apply(info.metric);
      p.values = ImmutableMap.of(metricName, (Object) info.value);
      Warning warning = warnings.get(info.metric);
      p.tags = Maps.newLinkedHashMap(); // We need the order!
      p.tags.put("service", info.service.replace("/", ":")); // Transform into real service name.
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
      if (shouldWarn && Config.config.druidInfos.shouldWarn(info.metric)) {
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

    Event toPoint(MetricInfo info) {
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

    List<Event> points = EmitMetricsAnalyzer.fetchReceivedInfos(parseMetrics(msg));

    //Resources.influxDB.write("dcmonitor", "", points);

    System.out.println(Resources.jsonMapper.writeValueAsString(points));
  }

}
