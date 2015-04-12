package com.sf.monitor.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Point;
import com.sf.log.Logger;
import com.sf.monitor.Config;
import com.sf.monitor.Resources;
import com.sf.notify.Notify;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Utils {
  private static final Logger log = new Logger(Utils.class);
  private static final Pattern regex = Pattern.compile("[. :/\\\\{}\\[\\]()*!@#$%^&*;',\"<>]");
  private static final String agentId = "2";

  public static Function<String, String> smoothText = new Function<String, String>() {
    @Override
    public String apply(String input) {
      return regex.matcher(input).replaceAll("_");
    }
  };

  public static Map<String, Object> toMap(String json) {
    if (json == null) {
      return null;
    }
    try {
      return Resources.jsonMapper.readValue(
        json, new TypeReference<Map<String, Object>>() {
        }
      );
    } catch (IOException e) {
      log.error(e, "cannot parse [%s]", json);
      return null;
    }
  }

  public static <T> T toObject(String json, Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return Resources.jsonMapper.readValue(
        json, clazz
      );
    } catch (IOException e) {
      log.error(e, "cannot parse [%s]", json);
      return null;
    }
  }

  public static void sendNotify(String type, String content) {
    if (!Config.config.notify.doSend) {
      return;
    }
    String appName = Config.config.notify.appName;
    String title = String.format("notify: %s - %s", appName, type);
    content = String.format("%s - %s", new DateTime().toString(), content);
    Resources.notify.send(new Notify.Wechat(title, content, "0", agentId), true);
    if (Config.config.notify.emails.size() != 0) {
      Resources.notify.send(
        new Notify.Email(
          Joiner.on(';').join(Config.config.notify.emails),
          title,
          content,
          "plain"
        ),
        true
      );
    }
    if (Config.config.notify.phones.size() != 0) {
      Resources.notify.send(
        new Notify.SMS(title, content, Joiner.on(',').join(Config.config.notify.phones)),
        true
      );
    }
  }

  public static List<Point> mergePoints(Iterable<Point> points, String tableName, String timestamp) {
    Map<String, Point> pointMap = Maps.newHashMap();
    for (Point p : points) {
      String key = Joiner.on("-").join(
        Iterables.transform(
          p.tags.entrySet(), new Function<Map.Entry<String, String>, String>() {
            @Override
            public String apply(Map.Entry<String, String> input) {
              return input.getKey() + ":" + input.getValue();
            }
          }
        )
      );
      Point oldPoint = pointMap.get(key);
      if (oldPoint == null) {
        Point newPoint = new Point();
        newPoint.name = tableName;
        newPoint.timestamp = timestamp;
        newPoint.tags = Maps.newHashMap(p.tags);
        newPoint.fields = Maps.newHashMap(p.fields);
        pointMap.put(key, newPoint);
      } else {
        for (Map.Entry<String, Object> e : p.fields.entrySet()) {
          oldPoint.fields.put(e.getKey(), e.getValue());
        }
        pointMap.put(key, oldPoint);
      }
    }
    return Lists.newArrayList(pointMap.values());
  }
}



