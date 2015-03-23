package com.sf.influxdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sf.influxdb.dto.Point;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.impl.InfluxDBImpl;

public class InfluxDBFactory {

  /**
   * Create a connection to a InfluxDB.
   *
   * @param url      the url to connect to.
   * @param username the username which is used to authorize against the influxDB instance.
   * @param password the password for the username which is used to authorize against the influxDB
   *                 instance.
   * @return a InfluxDB adapter suitable to access a InfluxDB.
   */
  public static InfluxDB connect(final String url, final String username, final String password) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "The URL may not be null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "The username may not be null or empty.");
    return new InfluxDBImpl(url, username, password);
  }

  public static void main(String[] args) throws Exception {
    InfluxDB db = connect("http://localhost:8086", "root", "root");

    Point p = new Point();
    p.name = "test_series";
    p.tags = ImmutableMap.of("t1", "ffff", "t2", "sggg");
    p.fields = Maps.newHashMap();
    p.fields.put("value", 8.9);
    Point p2 = new Point();
    p2.name = "test_series";
    p2.tags = ImmutableMap.of("t1", "ffff", "t2", "sggg");
    p2.fields = Maps.newHashMap();
    p2.fields.put("value", 8.9);

    db.write("test", "", Lists.newArrayList(p, p2));

    Results res = db.query("test", "select * from test_series");
    ObjectMapper ob = new ObjectMapper();
    ob.writeValue(System.out, res);
  }
}
