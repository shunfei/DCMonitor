package com.sf.monitor.zk;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZookeeperStatParser {
  public static Pattern LatencyRegx = Pattern.compile("Latency min/avg/max: \\d+/\\d+/\\d+");
  public static Pattern ReceivedRegx = Pattern.compile("Received: \\d+");
  public static Pattern SentRegx = Pattern.compile("Sent: \\d+");
  public static Pattern ConnectionsRegx = Pattern.compile("Connections: \\d+");
  public static Pattern OutstandingRegx = Pattern.compile("Outstanding: \\d+");
  public static Pattern ZxidRegx = Pattern.compile("Zxid: \\w+");
  public static Pattern ModeRegx = Pattern.compile("Mode: \\w+");
  public static Pattern NodeCountRegx = Pattern.compile("Node count: \\d+");

  public static Map<String, Integer> parseLatency(String statInfo) {
    Map<String, Integer> values = Maps.newHashMap();
    values.put("min", -1);
    values.put("avg", -1);
    values.put("max", -1);

    String vs = parseValueStr(LatencyRegx, statInfo);
    if (vs == null) {
      return values;
    }
    String[] ss = vs.split("/");
    if (ss.length != 3) {
      return values;
    }
    values.put("min", Integer.valueOf(ss[0]));
    values.put("avg", Integer.valueOf(ss[1]));
    values.put("max", Integer.valueOf(ss[2]));
    return values;
  }

  public static String parseStrWithDefault(Pattern p, String statInfo, String defaultVal) {
    String val = parseValueStr(p, statInfo);
    return val == null ? defaultVal : val;
  }

  private static String parseValueStr(Pattern p, String statInfo) {
    Matcher m = p.matcher(statInfo);
    if (!m.find()) {
      return null;
    }
    String s = m.group();
    int i = s.indexOf(": ");
    if (i == -1) {
      return null;
    }
    return s.substring(i + 2, s.length());
  }

  public static Integer parseIntWithDefault(Pattern p, String statInfo, Integer defaultVal) {
    Integer val = parseInt(p, statInfo);
    return val == null ? defaultVal : val;
  }

  private static Integer parseInt(Pattern p, String statInfo) {
    String vs = parseValueStr(p, statInfo);
    if (vs == null) {
      return null;
    }
    return Integer.parseInt(vs);
  }

  public static void main(String[] args) throws Exception {
    String s = IOUtils.toString(ClassLoader.getSystemResourceAsStream("zk_stat_msg"));
    System.out.println(parseLatency(s));
    System.out.println(parseValueStr(ZxidRegx, s));
    System.out.println(parseValueStr(ModeRegx, s));
    System.out.println(parseInt(SentRegx, s));
    System.out.println(parseInt(NodeCountRegx, s));
  }
}
