package com.sf.monitor.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Point;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaStats {
  private static final Logger log = new Logger(KafkaStats.class);
  public static final String tableName = "kafka_metrics";

  public static List<Point> fetchTrendInfos() {
    List<Point> points = Lists.newArrayList();

    fetchOffsets(points);

    return points;
  }

  private static void fetchOffsets(List<Point> points) {
    try {
      KafkaInfos.ActiveTopics activeInfos = Resources.kafkaInfos.getActiveTopicMap();
      Map<String, Set<String>> groupToTopic = activeInfos.consumerToTopic;
      for (Map.Entry<String, Set<String>> e : groupToTopic.entrySet()) {
        String group = e.getKey();
        Set<String> topics = e.getValue();
        List<KafkaInfos.OffsetInfo> offsetInfos = Resources.kafkaInfos.getConsumerInfos(group, topics);

        Map<String, TotalInfo> totalInfos = Maps.newHashMap();

        for (KafkaInfos.OffsetInfo info : offsetInfos) {
          points.add(createOffsetPoint(info.topic, info.group, info.partition, info.logSize, info.offset));

          TotalInfo totalInfo = totalInfos.get(info.topic);
          if (totalInfo == null) {
            totalInfo = new TotalInfo();
          }
          totalInfo.totalSize += info.logSize;
          totalInfo.totalOffset += info.offset;
          totalInfos.put(info.topic, totalInfo);
        }

        for (Map.Entry<String, TotalInfo> te : totalInfos.entrySet()) {
          String topic = te.getKey();
          TotalInfo totalInfo = te.getValue();
          points.add(createOffsetPoint(topic, group, -1, totalInfo.totalSize, totalInfo.totalOffset));

          long lag = totalInfo.totalSize - totalInfo.totalOffset;
          if (Config.config.kafka.shouldAlarm(topic, group, lag)) {
            String warnMsg = String.format(
              "topic:[%s],consumer:[%s] - consum lag: current[%d],threshold[%d], topic lag too long!",
              topic,
              group,
              lag,
              Config.config.kafka.getWarnLag(topic, group)
            );
            Utils.sendNotify("kafka", warnMsg);
            log.warn("kafka - " + warnMsg);
          }
        }
      }
    } catch (Exception e) {
      log.error(e, "");
    }
  }

  private static Point createOffsetPoint(String topic, String consumer, int partition, long logSize, long offset) {
    Point p = new Point();
    p.name = "kafka_consume";
    p.fields = ImmutableMap.<String, Object>of(
      "size",
      logSize,
      "offs", // Stupid influxdb cannot use key word, e.g. offset, as field name!
      offset,
      "lag",
      logSize - offset
    );
    p.tags = ImmutableMap.of(
      "topic",
      topic,
      "consumer",
      consumer,
      "partition",
      String.valueOf(partition)
    );
    return p;
  }

  private static class TotalInfo {
    long totalSize, totalOffset;
  }

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();

    System.out.printf("series: %s", Resources.jsonMapper.writeValueAsString(fetchTrendInfos()));

  }
}
