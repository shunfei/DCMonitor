package com.sf.monitor.kafka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Series;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import kafka.api.OffsetRequest;
import kafka.api.OffsetResponse;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.PartitionOffsetsResponse;
import kafka.api.Request;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import scala.Int;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Map.Map1;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaInfos {
  private static final Logger log = new Logger(KafkaInfos.class);

  private ZkClient zkClient;
  private Map<Integer, SimpleConsumer> consumerMap = Maps.newHashMap();

  public KafkaInfos(ZkClient zkClient) {
    this.zkClient = zkClient;
  }

  private SimpleConsumer getConsumer(Integer bid) {
    try {
      String brokerInfo = zkClient.readData(ZkUtils.BrokerIdsPath() + "/" + bid, true);
      if (brokerInfo == null) {
        log.error("Broker id %d does not exist", bid);
        return null;
      }
      Map<String, Object> map = Resources.jsonMapper.readValue(
        brokerInfo, new TypeReference<Map<String, Object>>() {
        }
      );
      String host = (String) map.get("host");
      Integer port = (Integer) map.get("port");
      return new SimpleConsumer(host, port, 10000, 100000, "KafkaConsumerInfos");
    } catch (Exception e) {
      log.error(e, "Could not parse broker[%d] info", bid);
      return null;
    }
  }

  private OffsetInfo processPartition(String group, String topic, int pid) {
    try {
      Stat stat = new Stat();
      String offsetStr = zkClient.readData(
        String.format(
          "%s/%s/offsets/%s/%d",
          ZkUtils.ConsumersPath(),
          group,
          topic,
          pid
        ), stat
      );
      Long offset = Long.valueOf(offsetStr);
      DateTime creation = new DateTime(stat.getCtime());
      DateTime modified = new DateTime(stat.getMtime());

      String owner = zkClient.readData(
        String.format(
          "%s/%s/owners/%s/%d",
          ZkUtils.ConsumersPath(),
          group,
          topic,
          pid
        ), true
      );

      Option<Object> o = ZkUtils.getLeaderForPartition(zkClient, topic, pid);
      if (o.get() == null) {
        log.error("No broker for partition %s - %s", topic, pid);
      }
      Integer leaderId = Int.unbox(o.get());
      SimpleConsumer consumer = consumerMap.get(leaderId);
      if (consumer == null) {
        consumer = getConsumer(leaderId);
      }
      // getConsumer may fail.
      if (consumer == null) {
        return null;
      }
      consumerMap.put(leaderId, consumer);
      TopicAndPartition topicAndPartition = new TopicAndPartition(topic, pid);
      PartitionOffsetRequestInfo requestInfo = new PartitionOffsetRequestInfo(OffsetRequest.LatestTime(), 1);
      OffsetRequest request = new OffsetRequest(
        new Map1<TopicAndPartition, PartitionOffsetRequestInfo>(topicAndPartition, requestInfo),
        0,
        Request.OrdinaryConsumerId()
      );
      OffsetResponse response = consumer.getOffsetsBefore(request);
      PartitionOffsetsResponse offsetsResponse = response.partitionErrorAndOffsets().get(topicAndPartition).get();
      Long logSize = scala.Long.unbox(offsetsResponse.offsets().head());

      OffsetInfo info = new OffsetInfo();
      info.group = group;
      info.topic = topic;
      info.partition = pid;
      info.offset = offset;
      info.logSize = logSize;
      info.lag = logSize - offset;
      info.owner = owner;
      info.creation = creation;
      info.modified = modified;
      info.creationTime = creation.toString();
      info.modifiedTime = modified.toString();
      return info;
    } catch (Exception e) {
      log.error(e, "Could not parse partition info. group: [%s] topic: [%s]", group, topic);
      return null;
    }
  }

  private List<OffsetInfo> processTopic(String group, String topic) {
    Seq<String> singleTopic = JavaConversions.asScalaBuffer(Collections.singletonList(topic)).toSeq();
    scala.collection.Map<String, Seq<Object>> pidMap = ZkUtils.getPartitionsForTopics(zkClient, singleTopic);
    Option<Seq<Object>> partitions = pidMap.get(topic);
    if (partitions.get() == null) {
      return Collections.emptyList();
    }
    List<OffsetInfo> infos = Lists.newArrayList();
    for (Object o : JavaConversions.asJavaList(partitions.get())) {
      OffsetInfo info = processPartition(group, topic, Int.unbox(o));
      if (info != null) {
        infos.add(info);
      }
    }
    return infos;
  }

  private List<BrokerInfo> brokerInfo() {
    List<BrokerInfo> infos = Lists.newArrayListWithExpectedSize(consumerMap.size());
    for (Map.Entry<Integer, SimpleConsumer> entry : consumerMap.entrySet()) {
      BrokerInfo info = new BrokerInfo();
      info.id = entry.getKey();
      info.host = entry.getValue().host();
      info.port = entry.getValue().port();
      infos.add(info);
    }
    return infos;
  }

  // Get the detail consumer information of given topics, if topics not specificed, i.e. null,
  // operate on all topics.
  public List<OffsetInfo> getConsumerInfos(String group, Set<String> topics) {
    List<String> topicList = null;
    if (topics != null && topics.size() > 0) {
      topicList = Lists.newArrayList(topics);
    } else {
      try {
        topicList = zkClient.getChildren(String.format("%s/%s/offsets", ZkUtils.ConsumersPath(), group));
        topicList = topicList == null ? Collections.<String>emptyList() : topicList;
      } catch (Exception e) {
        return Collections.emptyList();
      }
    }
    Collections.sort(topicList);
    List<OffsetInfo> infos = Lists.newArrayListWithExpectedSize(topicList.size());
    for (String topic : topicList) {
      infos.addAll(processTopic(group, topic));
    }
    return infos;
  }

  public List<String> getGroups() {
    try {
      List<String> list = zkClient.getChildren(ZkUtils.ConsumersPath());
      if (list == null) {
        return Collections.emptyList();
      } else {
        return list;
      }
    } catch (Exception e) {
      log.error(e, "could not get groups");
      return Collections.emptyList();
    }
  }

  public Set<String> getTopicConsumers(String topic) {
    return getActiveTopicMap().topicToConsumer.get(topic);
  }

  public List<String> getTopics() {
    try {
      return JavaConversions.asJavaList(ZkUtils.getAllTopics(zkClient));
    } catch (Exception e) {
      log.error(e, "could not get topics");
      return Collections.emptyList();
    }
  }

  // pair: topic to group & group to topic
  public ActiveTopics getActiveTopicMap() {
    Map<String, Set<String>> topicToGroup = Maps.newHashMap();
    Map<String, Set<String>> groupToTopic = Maps.newHashMap();

    List<String> consumers;
    try {
      consumers = JavaConversions.asJavaList(ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath()));
    } catch (Exception e) {
      log.error(e, "could not get all consumer list");
      return new ActiveTopics();
    }
    for (String group : consumers) {
      try {
        Set<String> topics = JavaConversions.asJavaMap(
          ZkUtils.getConsumersPerTopic(
            zkClient,
            group,
            true
          )
        ).keySet();

        for (String topic : topics) {
          Set<String> groups = topicToGroup.get(topic);
          if (groups == null) {
            groups = Sets.newHashSet();
            topicToGroup.put(topic, groups);
          }
          groups.add(group);
        }

        if (!topics.isEmpty()) {
          groupToTopic.put(group, topics);
        }
      } catch (Exception e) {
        log.error(e, "could not get consumers for group %s", group);
      }
    }
    ActiveTopics at = new ActiveTopics();
    at.topicToConsumer = topicToGroup;
    at.consumerToTopic = groupToTopic;
    return at;
  }

  public List<BrokerInfo> getCluster() {
    return Lists.transform(
      JavaConversions.asJavaList(ZkUtils.getAllBrokersInCluster(zkClient)), new Function<Broker, BrokerInfo>() {
        @Override
        public BrokerInfo apply(Broker input) {
          BrokerInfo info = new BrokerInfo();
          info.host = input.host();
          info.port = input.port();
          info.id = input.id();
          return info;
        }
      }
    );
  }

  // 获取topic消费历史数据
  public List<SimpleOffsetInfo> getTrendConsumeInfos(String consumer, String topic, DateTime from, DateTime to) {
    String fromStr = from.withZone(DateTimeZone.UTC).toString();
    String toStr = to.withZone(DateTimeZone.UTC).toString();
    String sql = String.format(
      "select size, offs, lag from kafka_consume "
      + "where consumer='%s' and topic='%s' and partition='-1' and time >= '%s' and time <= '%s' "
      + "group by topic, consumer",
      consumer,
      topic,
      fromStr,
      toStr
    );
    Results results = Resources.influxDB.query(
      Config.config.influxdb.influxdbDatabase,
      sql
    );
    Series series = results.getFirstSeries();
    if (series == null) {
      return Collections.emptyList();
    }
    return Lists.transform(
      series.indexedValues(), new Function<Map<String, Object>, SimpleOffsetInfo>() {
        @Override
        public SimpleOffsetInfo apply(Map<String, Object> row) {
          SimpleOffsetInfo info = new SimpleOffsetInfo();
          info.logSize = ((Double) row.get("size")).longValue();
          info.offset = ((Double) row.get("offs")).longValue();
          info.lag = ((Double) row.get("lag")).longValue();
          info.timeStr = (String)row.get("time");
          info.time = DateTime.parse(info.timeStr);
					info.timeStamp = info.time.getMillis();
          return info;
        }
      }
    );
  }

  public void close() {
    for (SimpleConsumer consumer : consumerMap.values()) {
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  public static class ActiveTopics {
    public Map<String, Set<String>> topicToConsumer = Collections.emptyMap();
    public Map<String, Set<String>> consumerToTopic = Collections.emptyMap();
  }

  public static class OffsetInfo {
    public String group;
    public String topic;
    public Integer partition;
    public Long offset;
    public Long logSize;
    public Long lag;
    public String owner;
    public String creationTime;
    public String modifiedTime;
    @JsonIgnore
    public DateTime creation;
    @JsonIgnore
    public DateTime modified;
  }

  public static class SimpleOffsetInfo {
    @JsonIgnore
    public DateTime time;
    public String timeStr;
    public long offset;
    public long logSize;
    public long lag;
		public long timeStamp;
	}

  public static class BrokerInfo {
    public Integer id;
    public String host;
    public Integer port;
  }

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();

    KafkaInfos checker = new KafkaInfos(Resources.zkClient);

    System.out.println("getActiveTopicMap: " + Resources.jsonMapper.writeValueAsString(checker.getActiveTopicMap()));
    System.out.println("getTopics: " + Resources.jsonMapper.writeValueAsString(checker.getTopics()));
    System.out.println(
      "getTopicConsumers: "
      + Resources.jsonMapper.writeValueAsString(checker.getTopicConsumers("clicki_stat_topic"))
    );
    System.out.println(
      "getConsumerInfos: " + Resources.jsonMapper.writeValueAsString(
        checker.getConsumerInfos("druid-real-time-node-bid", null)
      )
    );
    System.out.println("getCluster: " + Resources.jsonMapper.writeValueAsString(checker.getCluster()));

    DateTime to = new DateTime();

    DateTime from = to.minus(new Period("PT3H"));

    System.out.println(
      "getTrendOffsetInfos: " + Resources.jsonMapper.writeValueAsString(
        checker.getTrendConsumeInfos(
          "clicki_druid_ingester_0",
          "clicki_stat_topic",
          from,
          to
        )
      )
    );

  }
}
