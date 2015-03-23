package com.sf.monitor.utils;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.sf.log.Logger;
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

public class HttpRequest {
  private static Logger log = new Logger(HttpRequest.class);

  public static <T> T create(String url, Class<T> clazz) {
    try {
      String hostPart = new URI(url).getHost();
      // Check whether it is a valide url.
      InetAddress.getByName(hostPart);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("The given URI is not valid " + e.getMessage());
    }
    OkHttpClient okHttpClient = new OkHttpClient();
    RestAdapter restAdapter = new RestAdapter.Builder()
      .setEndpoint(url)
      .setErrorHandler(new HttpErrorHandler())
      .setClient(new OkClient(okHttpClient))
      .build();

    return restAdapter.create(clazz);
  }

  public static class HttpErrorHandler implements ErrorHandler {
    @Override
    public Throwable handleError(final RetrofitError cause) {
      Response r = cause.getResponse();
      if (r != null && r.getStatus() >= 400) {
        InputStreamReader reader = null;
        try {
          reader = new InputStreamReader(r.getBody().in(), Charsets.UTF_8);
          return new RuntimeException(CharStreams.toString(reader));
        } catch (IOException e) {
          log.error(e, "http error!");
        } finally {
          Closeables.closeQuietly(reader);
        }
      }
      return cause;
    }
  }
}
