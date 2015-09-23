
package com.sf.monitor.kafka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.monitor.utils.DCMZkUtils;
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
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.joda.time.DateTime;
import scala.Int;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Map.Map1;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaInfos implements Closeable {
  private static final Logger log = new Logger(KafkaInfos.class);

  private ZkClient zkClient;
  private Map<Integer, SimpleConsumer> consumerMap = Maps.newHashMap();

  public KafkaInfos(ZkClient zkClient) {
    this.zkClient = zkClient;
  }

  private SimpleConsumer createSimpleConsumer(Integer brokerId) {

    try {
      String brokerInfo = zkClient.readData(ZkUtils.BrokerIdsPath() + "/" + brokerId, true);
      if (brokerInfo == null) {
        log.error("Broker clientId %d does not exist", brokerId);
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
      log.error(e, "Could not parse broker[%d] info", brokerId);
      return null;
    }
  }

  private long getTopicLogSize(String topic, int pid) {
    Option<Object> o = ZkUtils.getLeaderForPartition(zkClient, topic, pid);
    if (o.isEmpty() || o.get() == null) {
      log.error("No broker for partition %s - %s", topic, pid);
      return 0;
    }
    Integer leaderId = Int.unbox(o.get());
    SimpleConsumer consumer = consumerMap.get(leaderId);
    if (consumer == null) {
      consumer = createSimpleConsumer(leaderId);
    }
    // createSimpleConsumer may fail.
    if (consumer == null) {
      return 0;
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
    return scala.Long.unbox(offsetsResponse.offsets().head());
  }

  private PartitionInfo getPartitionInfo(String group, String topic, int pid) {
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

      long logSize = getTopicLogSize(topic, pid);

      PartitionInfo info = new PartitionInfo();
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

  private PartitionInfo getStormKafkaPartitionInfo(final String clientId, final int pid) {
    try {
      final String zkRoot = Config.config.kafka.stormKafkaRoot;
      if (StringUtils.isEmpty(zkRoot)) {
        return null;
      }
      StormkafkaPartitionInfo pInfo;
      DateTime creation;
      DateTime modified;
      try {
        Stat stat = new Stat();
        String msg = zkClient.readData(
          String.format(
            "%s/%s/partition_%d",
            zkRoot,
            clientId,
            pid
          ), stat
        );
        creation = new DateTime(stat.getCtime());
        modified = new DateTime(stat.getMtime());
        pInfo = Resources.jsonMapper.readValue(msg, StormkafkaPartitionInfo.class);
      } catch (Exception e) {
        log.warn("Storm kafka clientId[%s], partition[%d] not found", clientId, pid);
        pInfo = new StormkafkaPartitionInfo();
        pInfo.offset = 0;
        pInfo.partition = 0;
        pInfo.topic = "unkown";
        pInfo.topology = new StormTopology();
        pInfo.topology.topoName = "unknown";
        pInfo.topology.workerId = "unknown";
        creation = new DateTime();
        modified = new DateTime();
      }

      long logSize = getTopicLogSize(pInfo.topic, pid);

      PartitionInfo info = new PartitionInfo();
      info.group = clientId;
      info.topic = pInfo.topic;
      info.partition = pid;
      info.offset = pInfo.offset;
      info.logSize = logSize;
      info.lag = logSize - pInfo.offset;
      info.owner = pInfo.topology.topoName + "|" + pInfo.topology.workerId;
      info.creation = creation;
      info.modified = modified;
      info.creationTime = creation.toString();
      info.modifiedTime = modified.toString();

      return info;
    } catch (Exception e) {
      log.warn(e, "Could not parse storm kafka partition info. clientId: [%s] topic: [%d]", clientId, pid);
      return null;
    }
  }

  public List<PartitionInfo> getStormkafkaPartitionInfos(final String clientId) {
    final String zkRoot = Config.config.kafka.stormKafkaRoot;
    if (StringUtils.isEmpty(zkRoot)) {
      return Collections.emptyList();
    }
    List<String> partitions = DCMZkUtils.getZKChildren(String.format("%s/%s", zkRoot, clientId));
    return Lists.transform(
      partitions, new Function<String, PartitionInfo>() {
        @Override
        public PartitionInfo apply(String input) {
          Integer partitionId = Integer.valueOf(input.split("_")[1]);
          return getStormKafkaPartitionInfo(clientId, partitionId);
        }
      }
    );
  }

  public List<PartitionInfo> getPartitionInfos(String group, String topic) {
    Seq<String> singleTopic = JavaConversions.asScalaBuffer(Collections.singletonList(topic)).toSeq();
    scala.collection.Map<String, Seq<Object>> pidMap = ZkUtils.getPartitionsForTopics(zkClient, singleTopic);
    Option<Seq<Object>> partitions = pidMap.get(topic);
    if (partitions.get() == null) {
      return Collections.emptyList();
    }
    List<PartitionInfo> infos = Lists.newArrayList();
    for (Object o : JavaConversions.asJavaList(partitions.get())) {
      PartitionInfo info = getPartitionInfo(group, topic, Int.unbox(o));
      if (info != null) {
        infos.add(info);
      }
    }
    return infos;
  }

  private List<BrokerInfo> getBrokerInfos() {
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

  public List<StormKafkaClientInfo> getStormKafkaClients() {
    final String zkRoot = Config.config.kafka.stormKafkaRoot;
    if (StringUtils.isEmpty(zkRoot)) {
      return Collections.emptyList();
    }
    List<String> ids = DCMZkUtils.getZKChildren(zkRoot);
    return Lists.transform(
      ids, new Function<String, StormKafkaClientInfo>() {
        @Override
        public StormKafkaClientInfo apply(String id) {
          StormKafkaClientInfo it = new StormKafkaClientInfo();
          it.clientId = id;
          List<PartitionInfo> infos = getStormkafkaPartitionInfos(id);
          for (PartitionInfo info : infos) {
            if (!"unkown".equals(info.topic)) {
              it.topic = info.topic;
              break;
            }
          }
          return it;
        }
      }
    );
  }

  @Override
  public void close() {
    for (SimpleConsumer consumer : consumerMap.values()) {
      if (consumer != null) {
        consumer.close();
      }
    }
    if (zkClient != null) {
      zkClient.close();
    }
  }

  public static class ActiveTopics {
    public Map<String, Set<String>> topicToConsumer = Collections.emptyMap();
    public Map<String, Set<String>> consumerToTopic = Collections.emptyMap();
  }

  public static class PartitionInfo {
    /**
     * kafka consumer group or storm kafka client clientId.
     */
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

  public static class BrokerInfo {
    public Integer id;
    public String host;
    public Integer port;
  }

  public static class StormKafkaClientInfo {
    public String clientId;
    public String topic;
  }

  public static class StormkafkaPartitionInfo {
    public StormTopology topology;
    public long offset;
    public int partition;
    public String topic;
  }

  public static class StormTopology {
    @JsonProperty("id")
    public String workerId;
    @JsonProperty("name")
    public String topoName;
  }

  //public static class StormKafka

  public static void main(String[] args) throws Exception {
    Config.init("config");
    Resources.init();

    KafkaInfos checker = Resources.kafkaInfos;

    System.out.println("getActiveTopicMap: " + Resources.jsonMapper.writeValueAsString(checker.getActiveTopicMap()));
    System.out.println("getTopics: " + Resources.jsonMapper.writeValueAsString(checker.getTopics()));
    System.out.println(
      "getTopicConsumers: "
      + Resources.jsonMapper.writeValueAsString(checker.getTopicConsumers("clicki_stat_topic"))
    );
    System.out.println("getCluster: " + Resources.jsonMapper.writeValueAsString(checker.getCluster()));
    System.out.println(
      "getPartitionInfo: " + Resources.jsonMapper.writeValueAsString(
        checker.getPartitionInfo(
          "ssp_mbv_druid_ingester_0",
          "ssp_mbv_stat_topic",
          0
        )
      )
    );
    System.out.println(
      "getStormKafkaPartitionInfo: " + Resources.jsonMapper.writeValueAsString(
        checker.getStormkafkaPartitionInfos(
          "clicki_track_storm"
        )
      )
    );
    System.out.println(
      "getStormKafkaClients: " + Resources.jsonMapper.writeValueAsString(
        checker.getStormKafkaClients()
      )
    );
    System.out.println(
      "getStormkafkaPartitionInfos: " + Resources.jsonMapper.writeValueAsString(
        checker.getStormkafkaPartitionInfos("clicki_track_storm")
      )
    );

    System.out.println(
      "fetchCurrentInfos: " + Resources.jsonMapper.writeValueAsString(
        KafkaStats.fetchCurrentInfos()
      )
    );
  }
}
