package com.sf.monitor.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.influxdb.Event;
import com.sf.monitor.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaStats {
  private static final Logger log = new Logger(KafkaStats.class);
  public static final String tableName = "kafka_metrics";

  public static List<Event> fetchCurrentInfos() {
    List<Event> points = Lists.newArrayList();

    fetchKafkaPartitionInfos(points);
    fetchStormKafkaPartitionInfos(points);

    return points;
  }

  private static void fetchKafkaPartitionInfos(List<Event> points) {
    try {
      KafkaInfos.ActiveTopics activeInfos = Resources.kafkaInfos.getActiveTopicMap();
      Map<String, Set<String>> groupToTopic = activeInfos.consumerToTopic;
      for (Map.Entry<String, Set<String>> e : groupToTopic.entrySet()) {
        String group = e.getKey();
        Set<String> topics = e.getValue();
        for (String topic : topics) {
          parsePartitionInfos(topic, group, Resources.kafkaInfos.getPartitionInfos(group, topic), points);
        }
      }
    } catch (Exception e) {
      log.error(e, "");
    }
  }

  private static void fetchStormKafkaPartitionInfos(List<Event> points) {
    try {
      List<KafkaInfos.StormKafkaClientInfo> infos = Resources.kafkaInfos.getStormKafkaClients();
      for (KafkaInfos.StormKafkaClientInfo info : infos) {
        parsePartitionInfos(
          info.topic,
          info.clientId,
          Resources.kafkaInfos.getStormkafkaPartitionInfos(info.clientId),
          points
        );
      }
    } catch (Exception e) {
      log.error(e, "");
    }
  }

  private static void parsePartitionInfos(
    String topic,
    String consumer,
    List<KafkaInfos.PartitionInfo> infos,
    List<Event> points
  ) {
    long totalSize = 0;
    long totalOffset = 0;
    for (KafkaInfos.PartitionInfo info : infos) {
      totalSize += info.logSize;
      totalOffset += info.offset;

      points.add(createPoint(info.topic, info.group, info.partition, info.logSize, info.offset));
    }

    points.add(createPoint(topic, consumer, -1, totalSize, totalOffset));

    long lag = totalSize - totalOffset;
    if (Config.config.kafka.shouldAlarm(topic, consumer, lag)) {
      String warnMsg = String.format(
        "topic:[%s],consumer:[%s] - consum lag: current[%d],threshold[%d], topic lag illegal!",
        topic,
        consumer,
        lag,
        Config.config.kafka.getWarnLag(topic, consumer)
      );
      Utils.sendNotify("kafka", warnMsg);
      log.warn("kafka - " + warnMsg);
    }
  }

  private static Event createPoint(String topic, String consumer, int partition, long logSize, long offset) {
    Event p = new Event();
    p.name = tableName;
    p.values = ImmutableMap.<String, Object>of(
      "size",
      logSize,
      "offs", // Stupid influxdb cannot use key word, e.g. offset, as field topoName!
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

    System.out.printf("series: %s", Resources.jsonMapper.writeValueAsString(fetchCurrentInfos()));

  }
}
