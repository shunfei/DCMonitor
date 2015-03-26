package com.sf.monitor;

public interface InfoFetcher {
  public static final String Druid = "druid";
  public static final String Zookeeper = "zookeeper";
  public static final String Kafka = "kafka";

  public String type();

  public void start() throws Exception;

  public void stop();

}
