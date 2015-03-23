package com.sf.influxdb.impl;

import com.sf.influxdb.InfluxDB;
import com.sf.influxdb.dto.Point;
import com.sf.influxdb.dto.Results;
import com.sf.influxdb.dto.Write;
import com.sf.log.Logger;
import com.squareup.okhttp.OkHttpClient;
import retrofit.RestAdapter;
import retrofit.client.OkClient;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;

public class InfluxDBImpl implements InfluxDB {
  private static final Logger log = new Logger(InfluxDBImpl.class);
  private static final int max_batch_points = 20;

  private final String username;
  private final String password;
  private final RestAdapter restAdapter;
  private final InfluxDBService influxDBService;

  /**
   * Constructor which should only be used from the InfluxDBFactory.
   *
   * @param url      the url where the influxdb is accessible.
   * @param username the user to connect.
   * @param password the password for this user.
   */
  public InfluxDBImpl(final String url, final String username, final String password) {
    super();
    this.username = username;
    this.password = password;
    try {
      String hostPart = new URI(url).getHost();
      InetAddress.getByName(hostPart);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    }
    OkHttpClient okHttpClient = new OkHttpClient();
    this.restAdapter = new RestAdapter.Builder()
      .setEndpoint(url)
      .setErrorHandler(new InfluxDBErrorHandler())
      .setClient(new OkClient(okHttpClient))
      .build();

    this.influxDBService = this.restAdapter.create(InfluxDBService.class);
  }

  @Override
  public InfluxDB setLogLevel(final InfluxDB.LogLevel logLevel) {
    switch (logLevel) {
      case NONE:
        this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.NONE);
        break;
      case BASIC:
        this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.BASIC);
        break;
      case HEADERS:
        this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.HEADERS);
        break;
      case FULL:
        this.restAdapter.setLogLevel(retrofit.RestAdapter.LogLevel.FULL);
        break;
      default:
        break;
    }
    return this;
  }

  @Override
  public void write(String database, String retentionPolicy, List<Point> points) {
    try {
      for (int i = 0; i < points.size(); i += max_batch_points) {
        int toIndex = i + max_batch_points;
        if (toIndex > points.size()) {
          toIndex = points.size();
        }
        influxDBService.write(username, password, new Write(database, retentionPolicy, points.subList(i, toIndex)));
      }
    } catch (Exception e) {
      log.error(
        e,
        String.format(
          "database: [%s], retentionPolicy: [%s], points: [%d]",
          database,
          retentionPolicy,
          points.size()
        )
      );
    }
  }

  @Override
  public Results query(String database, String command) {
    try {
      return influxDBService.query(username, password, database, command);
    } catch (Exception e) {
      log.error(e, String.format("database: [%s], command: [%s]", database, command));
      Results res = new Results();
      res.err = e.getMessage();
      return res;
    }
  }

}
