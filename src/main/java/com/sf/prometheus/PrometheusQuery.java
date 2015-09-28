package com.sf.prometheus;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.squareup.okhttp.OkHttpClient;
import retrofit.ErrorHandler;
import retrofit.RestAdapter;
import retrofit.RetrofitError;
import retrofit.client.OkClient;
import retrofit.client.Response;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class PrometheusQuery {

  private static class MyErrorHandler implements ErrorHandler {
    @Override
    public Throwable handleError(final RetrofitError cause) {
      Response r = cause.getResponse();
      if (r != null && r.getStatus() >= 400) {
        InputStreamReader reader = null;
        try {
          reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8);
          return new RuntimeException(CharStreams.toString(reader));
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          Closeables.closeQuietly(reader);
        }
      }
      return cause;
    }
  }

  private PrometheusQueryService agent;

  public PrometheusQuery(String url) throws RuntimeException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "The URL may not be null or empty.");
    try {
      String hostPart = new URI(url).getHost();
      // Validate the url.
      InetAddress.getByName(hostPart);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    }
    OkHttpClient okHttpClient = new OkHttpClient();
    RestAdapter restAdapter = new RestAdapter.Builder()
      .setEndpoint(url)
      .setErrorHandler(new MyErrorHandler())
      .setClient(new OkClient(okHttpClient))
      .build();
    this.agent = restAdapter.create(PrometheusQueryService.class);
  }

  public PrometheusQueryService.Result query(String query, long startMS, long endMS) {
    return agent.query(
      query,
      String.valueOf(startMS / 1000),
      String.valueOf(endMS / 1000),
      "5s"
    );
  }
}
