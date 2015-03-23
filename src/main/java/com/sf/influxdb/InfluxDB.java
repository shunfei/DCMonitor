package com.sf.influxdb;

import com.sf.influxdb.dto.Point;
import com.sf.influxdb.dto.Results;

import java.util.List;

public interface InfluxDB {
  /**
   * Controls the level of logging of the REST layer.
   */
  public enum LogLevel {
    /**
     * No logging.
     */
    NONE,
    /**
     * Log only the request method and URL and the response status code and execution time.
     */
    BASIC,
    /**
     * Log the basic information along with request and response headers.
     */
    HEADERS,
    /**
     * Log the headers, body, and metadata for both requests and responses.
     * <p/>
     * Note: This requires that the entire request and response body be buffered in memory!
     */
    FULL;
  }

  /**
   * Set the loglevel which is used for REST related actions.
   *
   * @param logLevel the loglevel to set.
   * @return the InfluxDB instance to be able to use it in a fluent manner.
   */
  public InfluxDB setLogLevel(final LogLevel logLevel);

  public void write(String database, String retentionPolicy, List<Point> points);

  public Results query(String database, String command);

}
