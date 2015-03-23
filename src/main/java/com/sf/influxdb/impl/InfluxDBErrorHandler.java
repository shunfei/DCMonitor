package com.sf.influxdb.impl;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.sf.log.Logger;
import retrofit.ErrorHandler;
import retrofit.RetrofitError;
import retrofit.client.Response;

import java.io.IOException;
import java.io.InputStreamReader;

class InfluxDBErrorHandler implements ErrorHandler {
  private static final Logger log = new Logger(InfluxDBErrorHandler.class);

  @Override
  public Throwable handleError(final RetrofitError cause) {
    Response r = cause.getResponse();
    if (r != null && r.getStatus() >= 400) {
      InputStreamReader reader = null;
      try {
        reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8);
        return new RuntimeException(CharStreams.toString(reader));
      } catch (IOException e) {
        log.error(e, "");
      } finally {
        Closeables.closeQuietly(reader);
      }
    }
    return cause;
  }
}
