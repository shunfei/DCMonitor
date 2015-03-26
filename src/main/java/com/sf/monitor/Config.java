package com.sf.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.sf.log.Logger;
import com.sf.monitor.druid.DruidInfoFetcher;
import com.sf.monitor.druid.DruidInfos;
import com.sf.monitor.kafka.KafkaInfoFetcher;
import com.sf.monitor.zk.ZookeeperInfoFetcher;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Config {
  private static final Logger log = new Logger(Config.class);

  @JsonProperty
  public InfluxdbConfig influxdb;
  @JsonProperty
  public ZKConfig zookeeper;
  @JsonProperty("druid")
  public DruidInfos druidInfos;
  @JsonProperty
  public KafkaConfig kafka;
  @JsonProperty
  public NotifyConfig notify;
  @JsonProperty
  public Fetchers fetchers;

  public List<InfoFetcher> fetcherList() {
    return ImmutableList.of(fetchers.druidFetcher, fetchers.kafkaFetcher, fetchers.zookeeperFetcher);
  }

  public static class InfluxdbConfig {
    @JsonProperty
    public String influxdbUrl;
    @JsonProperty
    public String influxdbDatabase;
    @JsonProperty
    public String influxdbUser;
    @JsonProperty
    public String influxdbPassword;
  }

  public static class ZKConfig {
    @JsonProperty
    public String addrs;
    @JsonProperty
    public int connectionTimeout;
  }

  public static class NotifyConfig {
    @JsonProperty
    public boolean doSend;
    @JsonProperty
    public String appName;
    @JsonProperty
    public String url;
    @JsonProperty
    public List<String> emails;
    @JsonProperty
    public List<String> phones;
  }

  public static class KafkaConfig {
    @JsonProperty
    public boolean warning;
    @JsonProperty
    public long warnDefaultLag = 100000;
    @JsonProperty
    public Map<String, Long> warnLagSpec;

    public long getWarnLag(String topic, String consumer) {
      if (warnLagSpec == null) {
        return warnDefaultLag;
      }
      Long lag = warnLagSpec.get(topic + "|" + consumer);
      return lag != null ? lag : warnDefaultLag;
    }
  }

  public static class Fetchers {
    @JsonProperty
    public DruidInfoFetcher druidFetcher;
    @JsonProperty
    public KafkaInfoFetcher kafkaFetcher;
    @JsonProperty
    public ZookeeperInfoFetcher zookeeperFetcher;
  }

  public static Config config;

  public static void init(String configDir) {
    try {
      config = mapConfig(configDir + "/config.json", Config.class);
    } catch (IOException e) {
      log.error(e, "Initial configuration failed!");
      System.exit(-1);
    }
  }

  private static <T> T mapConfig(String path, Class<T> clazz) throws IOException {
    File configFile = new File(path);
    if (!configFile.isFile() && !configFile.canRead()) {
      log.error("config file[%s] invalid!", path);
      throw new RuntimeException(String.format("config file[%s] invalid!", path));
    }
    return Resources.jsonMapper.readValue(configFile, clazz);
  }

  private static <T> T mapConfig(String path, TypeReference ref) throws IOException {
    File configFile = new File(path);
    if (!configFile.isFile() && !configFile.canRead()) {
      log.error("config file[%s] invalid!", path);
      throw new RuntimeException(String.format("config file[%s] invalid!", path));
    }

    return Resources.jsonMapper.readValue(configFile, ref);
  }
}
