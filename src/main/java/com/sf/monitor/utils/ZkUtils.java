package com.sf.monitor.utils;

import com.google.common.collect.Lists;
import com.sf.log.Logger;
import com.sf.monitor.Resources;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class ZkUtils {
  private static final Logger log = new Logger(ZkUtils.class);

  public static List<Map<String, Object>> getZKChildContentAsMap(String path, boolean tryDecompress) {
    List<Map<String, Object>> lists = Lists.newArrayList();
    for (String content : getZKChildContentOf(path, tryDecompress)) {
      Map<String, Object> map = Utils.toMap(content);
      if (map == null) {
        continue;
      }
      lists.add(map);
    }
    return lists;
  }

  public static List<String> getZKChildContentOf(String path, boolean tryDecompress) {
    List<String> children = getZKChildOf(path);
    List<String> contents = Lists.newArrayListWithCapacity(children.size());
    for (String child : children) {
      String content = readZKData(path + '/' + child, tryDecompress);
      if (content != null) {
        contents.add(content);
      }
    }
    return contents;
  }

  public static List<String> getZKChildOf(String path) {
    try {
      List<String> list = Resources.zkClient.getChildren(path);
      return list == null ? Collections.<String>emptyList() : list;
    } catch (Exception e) {
      log.error(e, "read children of [%s] from zookeeper failed!", path);
      return Collections.emptyList();
    }
  }

  public static String readZKData(String path, boolean tryDecompress) {
    if (tryDecompress) {
      byte[] bytes = null;
      try {
        // Make the serializer return the bytes directly.
        Resources.isZKDirectBytes.set(true);
        bytes = (byte[]) Resources.zkClient.readData(path, true);
      } catch (Throwable t) {
        log.error(t, "error");
      } finally {
        Resources.isZKDirectBytes.set(false);
      }
      if (bytes == null) {
        return null;
      }
      return decompress(bytes);
    } else {
      return Resources.zkClient.readData(path, true);
    }
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
