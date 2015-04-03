package com.sf.monitor.utils;

import com.google.common.collect.Lists;
import com.sf.log.Logger;
import com.sf.monitor.Resources;
import org.apache.commons.io.Charsets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class ZkUtils {
  private static final Logger log = new Logger(ZkUtils.class);

  public static String getLeader(String electionPath) {
     List<String> children = getZKChildren(electionPath);
     if (children.size() == 0) {
       return null;
     }
    String leader = children.get(0);
    for (String child : children) {
      if (child.length() < 10) {
        continue;
      }
      String leaderSNum = leader.substring(leader.length() - 10, leader.length());
      String curSNum = child.substring(child.length() - 10, child.length());
      if (curSNum.compareTo(leaderSNum) < 0) {
        leader = child;
      }
    }
    return leader;
  }

  public static String getLeaderContent(String electionPath) {
    String leader = getLeader(electionPath);
    if(leader == null) {
      return null;
    }
    return readZKData(electionPath + "/" + leader, false);
  }

  public static List<Map<String, Object>> getZKChildContentAsMap(String path, boolean tryDecompress) {
    List<Map<String, Object>> lists = Lists.newArrayList();
    for (String content : getZKChildrenContent(path, tryDecompress)) {
      Map<String, Object> map = Utils.toMap(content);
      if (map == null) {
        continue;
      }
      lists.add(map);
    }
    return lists;
  }

  public static List<String> getZKChildrenContent(String path, boolean tryDecompress) {
    List<String> children = getZKChildren(path);
    List<String> contents = Lists.newArrayListWithCapacity(children.size());
    for (String child : children) {
      String content = readZKData(path + '/' + child, tryDecompress);
      if (content != null) {
        contents.add(content);
      }
    }
    return contents;
  }

  public static List<String> getZKChildren(String path) {
    try {
      List<String> list = Resources.curator.getChildren().forPath(path);
      return list == null ? Collections.<String>emptyList() : list;
    } catch (Exception e) {
      log.error(e, "read children of [%s] from zookeeper failed!", path);
      return Collections.emptyList();
    }
  }

  public static String readZKData(String path, boolean tryDecompress) {
    byte[] bytes = null;
    try {
      if (tryDecompress) {
        bytes = Resources.curator.getData().decompressed().forPath(path);
      } else {
        bytes = Resources.curator.getData().forPath(path);
      }
    } catch (Throwable t) {
      log.error(t, "error");
      return null;
    }
    return new String(bytes, Charsets.UTF_8);
  }

  public static String decompress(byte[] compressedData) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(compressedData.length);
      GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(compressedData));
      byte[] buffer = new byte[compressedData.length];
      for (; ; ) {
        int bytesRead = in.read(buffer, 0, buffer.length);
        if (bytesRead < 0) {
          break;
        }
        bytes.write(buffer, 0, bytesRead);
      }
      return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    } catch (Throwable e) {
      log.debug(e, "try decompress failed.");
      return new String(compressedData, StandardCharsets.UTF_8);
    }
  }
}
