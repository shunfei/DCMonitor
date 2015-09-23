package com.sf.monitor.controllers;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sf.monitor.Event;
import com.sf.monitor.Resources;
import com.sf.monitor.kafka.KafkaInfos;
import com.sf.monitor.kafka.KafkaStats;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: sundy
 * @since 2015-03-02.
 */
@Controller
@RequestMapping("/kafka")
public class KafkaController {
  private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  @Value("${kafka.query.time.offset:PT15m}")
  private String timeOffset;

  @Value("${kafka.query.flushInterval:10000}")
  private String period;

  @Value("${timeZoneOffsetHour:0}")
  private int timeZoneOffsetHour;

  @RequestMapping("/hosts")
  public
  @ResponseBody
  List<KafkaInfos.BrokerInfo> hosts() {
    return Resources.kafkaInfos.getCluster();
  }

  @RequestMapping("/active")
  public
  @ResponseBody
  List<Map<String, String>> active() {
    KafkaInfos.ActiveTopics topics = Resources.kafkaInfos.getActiveTopicMap();
    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    for (Map.Entry<String, Set<String>> e : topics.topicToConsumer.entrySet()) {
      String topic = e.getKey();
      Set<String> consumers = e.getValue();
      for (String consumer : consumers) {
        list.add(ImmutableMap.of("Topic", topic, "Consumer", consumer));
      }
    }
    return list;
  }

  @RequestMapping("/consumer")
  public
  @ResponseBody
  List<Map<String, String>> consumer() {
    return Lists.transform(
      Resources.kafkaInfos.getGroups(),
      new Function<String, Map<String, String>>() {
        @Override
        public Map<String, String> apply(String input) {
          return ImmutableMap.of("Consumers", input);
        }
      }
    );
  }

  @RequestMapping("/storm_kafka")
  public
  @ResponseBody
  List<Map<String, String>> stormkafka() {
    return Lists.transform(
      Resources.kafkaInfos.getStormKafkaClients(),
      new Function<KafkaInfos.StormKafkaClientInfo, Map<String, String>>() {
        @Override
        public Map<String, String> apply(KafkaInfos.StormKafkaClientInfo info) {
          return ImmutableMap.of("Topic", info.topic, "ClientId", info.clientId);
        }
      }
    );
  }

  @RequestMapping("/topic")
  public
  @ResponseBody
  List<Map<String, String>> topic() {
    return Lists.transform(
      Resources.kafkaInfos.getTopics(),
      new Function<String, Map<String, String>>() {
        @Override
        public Map<String, String> apply(String input) {
          return ImmutableMap.of("Topics", input);
        }
      }
    );
  }

  @RequestMapping("/topic/{topic}")
  public String topicDetailHtml(@PathVariable String topic, String consumer, String type, Integer partitionId, Map<String, Object> mp) {
    mp.put("topic", topic);
    mp.put("consumer", consumer);
    mp.put("period", period);
    mp.put("timeZoneOffsetHour", timeZoneOffsetHour);
    mp.put("type", type);
    mp.put("partitionId", partitionId);
    return "topic_consumer";
  }

  @RequestMapping("/detail")
  public
  @ResponseBody
  Map<String, List<Event>> topicDetail(String topic, String consumer, String from, String to, Integer partitionId) {
    DateTime fromDate = null;
    DateTime toDate = null;
    DateTime now = new DateTime();
    try {
      fromDate = DateTime.parse(from, formatter);
    } catch (Exception e) {
      fromDate = now;
    }
    try {
      toDate = DateTime.parse(to, formatter);
    } catch (Exception e) {
      toDate = now;
    }
    if (from == null || to.equals(from)) {
      fromDate = toDate.minus(new Period(timeOffset));
    }
    partitionId = (partitionId == null) ? -1 : partitionId;
    return KafkaStats.getTrendConsumeInfos(consumer, topic, partitionId, fromDate, toDate);
  }

  @RequestMapping("/consumer_info")
  public
  @ResponseBody
  List<KafkaInfos.PartitionInfo> consumerInfos(final String topic, String consumer) {
    return Resources.kafkaInfos.getPartitionInfos(consumer, topic);
  }

  @RequestMapping("/storm_kafka_consumer_info")
  public
  @ResponseBody
  List<KafkaInfos.PartitionInfo> stormKafkaConsumerInfo(final String clientId) {
    return Resources.kafkaInfos.getStormkafkaPartitionInfos(clientId);
  }

}

