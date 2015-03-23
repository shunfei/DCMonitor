package com.sf.monitor.druid;

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

public class DruidReceivedMsg {
  private static final Logger log = new Logger(DruidReceivedMsg.class);

  public static List<Point> fetchReceivedInfos(List<MetricInfo> rawInfos) {
    List<Point> points = Lists.newArrayList();
    for (MetricInfo info : rawInfos) {
      Point p = metricFetchers.toPoint(info);
      if (p != null) {
        points.add(p);
      }
    }
    return points;
  }

  private static MetricFetchers metricFetchers = new MetricFetchers(
    new MetricFetcher("sys/mem").withKeys("max", "used").withUsers("user1", "type"),
    new MetricFetcher("sys/fs").withKeys("max", "used").withUsers("user1", "device", "user2", "mount"),
    new MetricFetcher("sys/disk").withKeys("write/count", "write/size", "read/count", "read/size")
                                 .withUsers("user1", "device", "user2", "mount"),
    new MetricFetcher("sys/net").withKeys("write/size", "read/size")
                                .withUsers("user1", "device", "user2", "ip", "user3", "ether"),
    new MetricFetcher("sys/cpu").withUsers("user1", "cpuid", "user2", "type"),
    new MetricFetcher("sys/swap").withKeys("max", "free", "pageIn", "pageOut"),
    new MetricFetcher("jvm/mem").withKeys("committed", "init", "max", "used").withUsers("user1", "type"),
    new MetricFetcher("jvm/pool").withKeys("committed", "init", "max", "used")
                                 .withUsers("user1", "type", "user2", "part"),
    new MetricFetcher("jvm/gc").withKeys("time", "count").withUsers("user1", "type"),
    new MetricFetcher("jvm/bufferpool").withKeys("capacity", "used").withUsers("user1", "type"),
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
    ),

    new MetricFetcher("server/segment").withKeys("used", "usedPercent", "count").withUsers(
      "user1",
      "datasource",
      "user2",
      "tier"
    ),
    new MetricFetcher("server/segment").withKeys("max"),
    new MetricFetcher("server/segment").withKeys("totalUsed", "totalCount", "totalUsedPercent")
                                       .withUsers("user2", "tier")
                                       .withWarning(
                                         "totalUsedPercent", new Warning(
                                           0.85,
                                           //0,
                                           "running out of segment space"
                                         )
                                       )

  );

  private static class MetricFetcher {
    final String name;
    List<String> keys;
    Map<String, String> userMap = Maps.newHashMap();
    Map<String, Warning> warnings = Maps.newHashMap();

    MetricFetcher(String name) {
      this.name = StringUtils.removeStart(StringUtils.removeEnd(name, "/"), "/");
    }

    MetricFetcher withKeys(String... keys) {
      this.keys = Lists.transform(
        Lists.newArrayList(keys), new Function<String, String>() {
          @Override
          public String apply(String key) {
            return StringUtils.removeStart(StringUtils.removeEnd(key, "/"), "/");
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
      if (keys == null) {
        return Collections.singleton(name);
      } else {
        return Iterables.transform(
          keys, new Function<String, String>() {
            @Override
            public String apply(String key) {
              return name + "/" + key;
            }
          }
        );
      }
    }

    Point toPoint(MetricInfo info) {
      Point p = new Point();
      p.name = "druid_" + Utils.smoothText.apply(name);
      Warning warning = null;
      if (keys == null) {
        p.fields = ImmutableMap.of(name, (Object) info.value);
        warning = warnings.get(name);
      } else {
        for (String key : keys) {
          if ((name + "/" + key).equals(info.metric)) {
            String colKey = Utils.smoothText.apply(key);
            p.fields = ImmutableMap.of(colKey, (Object) info.value);
            warning = warnings.get(key);
          }
        }
      }
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

    List<Point> points = DruidReceivedMsg.fetchReceivedInfos(parseMetrics(msg));

    Resources.influxDB.write("dc_monitor", "", points);

    System.out.println(Resources.jsonMapper.writeValueAsString(points));
  }

}
