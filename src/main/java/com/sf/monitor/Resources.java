package com.sf.monitor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.sf.log.Logger;
import com.sf.monitor.druid.DruidInfos;
import com.sf.monitor.kafka.KafkaInfos;
import com.sf.monitor.influxdb.Event;
import com.sf.monitor.influxdb.InfluxDBUtils;
import com.sf.monitor.zk.ZookeeperHosts;
import com.sf.notify.Notify;
import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.concurrent.TimeUnit;

public class Resources {
  private static final Logger log = new Logger(Resources.class);
  public static ThreadLocal<Boolean> isZKDirectBytes = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  private static final ZkSerializer zkSerializer = new ZkSerializer() {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ZKStringSerializer.serialize(data);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      if (isZKDirectBytes.get()) {
        return bytes;
      } else {
        return ZKStringSerializer.deserialize(bytes);
      }
    }
  };

  public static CuratorFramework curator;
  public static KafkaInfos kafkaInfos;
  public static DruidInfos druidInfos;
  public static ZookeeperHosts zkHosts;
  public static InfluxDB influxDB;
  public static Notify notify;
  public static Config.Fetchers fetchers;

  public static final ObjectMapper jsonMapper = new ObjectMapper();

  static {
    jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, false);
  }

  public static void init() {
    try {
      Config.CuratorConfig zkConfig = Config.config.zookeeper;
      curator = CuratorFrameworkFactory
        .builder()
        .connectString(zkConfig.addrs)
        .retryPolicy(new ExponentialBackoffRetry(zkConfig.baseSleepTimeMs, zkConfig.maxRetries, zkConfig.maxSleepMs))
        .compressionProvider(new GzipCompressionProvider())
        .build();
      curator.start();
      kafkaInfos = new KafkaInfos(new ZkClient(zkConfig.addrs, 30000, zkConfig.connectionTimeout, zkSerializer));
      druidInfos = Config.config.druidInfos;
      zkHosts = new ZookeeperHosts(Config.config.zookeeper.addrs);
      druidInfos.init();
      influxDB = InfluxDBFactory.connect(
        Config.config.influxdb.influxdbUrl,
        Config.config.influxdb.influxdbUser,
        Config.config.influxdb.influxdbPassword
      );
      influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
      tryCreateDb();
      notify = new Notify(Config.config.notify.url);
      fetchers = Config.config.fetchers;
    } catch (RuntimeException e) {
      log.error(e, "error while initialyzing resourses");
      throw e;
    }
  }

  private static void tryCreateDb() {
    String dbName = Config.config.influxdb.influxdbDatabase;

    QueryResult result = influxDB.query(new Query("SHOW DATABASES", dbName));
    QueryResult.Series series = InfluxDBUtils.getFirstSeries(result);
    if (series != null) {
      for (Event e : Event.fromSeries(series)){
        if ( StringUtils.equals(dbName, (String)e.values.get("name"))){
          log.info("Database [%s] already exists!", dbName);
          return;
        }
      }
    }
    log.info("Creating database [%s] in influxdb...", dbName);

    influxDB.createDatabase(dbName);
    influxDB.query(
      new Query(
        String.format("CREATE RETENTION POLICY seven_days ON %s DURATION 168h REPLICATION 1 DEFAULT", dbName),
        dbName
      )
    );
  }

  public static void close() {
    IOUtils.closeQuietly(kafkaInfos);
    IOUtils.closeQuietly(curator);
  }
}
