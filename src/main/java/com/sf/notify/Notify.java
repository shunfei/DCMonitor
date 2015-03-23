package com.sf.notify;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.gson.annotations.SerializedName;
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

public class Notify {
  public static class Email {
    @SerializedName("To")
    public String to;
    @SerializedName("Subject")
    public String subject;
    @SerializedName("Body")
    public String body;
    @SerializedName("MailType")
    public String mailType;

    public Email(String to, String subject, String body, String mailType) {
      this.to = to;
      this.subject = subject;
      this.body = body;
      this.mailType = mailType;
    }
  }

  public static class SMS {
    @SerializedName("ShortMsg")
    public String shortMsg;
    @SerializedName("Msg")
    public String msg;
    @SerializedName("To")
    public String to;

    public SMS(String shortMsg, String msg, String to) {
      this.shortMsg = shortMsg;
      this.msg = msg;
      this.to = to;
    }
  }

  public static class Wechat {
    @SerializedName("Head")
    public String head;
    @SerializedName("Content")
    public String content;
    @SerializedName("Safe")
    public String safe;
    @SerializedName("AgentId")
    public String agentId;

    public Wechat(String head, String content, String safe, String agentId) {
      this.head = head;
      this.content = content;
      this.safe = safe;
      this.agentId = agentId;
    }
  }

  public static class Result {
    @SerializedName("success")
    public boolean success;
    @SerializedName("message")
    public String message;
  }

  private static class NotifyErrorHandler implements ErrorHandler {
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

  private NotifyService service;

  public Notify(String url) throws RuntimeException {
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
      .setErrorHandler(new NotifyErrorHandler())
      .setClient(new OkClient(okHttpClient))
      .build();
    this.service = restAdapter.create(NotifyService.class);
  }

  /**
   * Send email notification, synchronized if sync is absent.
   */
  public Result send(Email email, boolean... sync) {
    return service.send(email, boolToInt(true, sync));
  }

  /**
   * Send sms notification, synchronized if sync is absent.
   */
  public Result send(SMS sms, boolean... sync) {
    return service.send(sms, boolToInt(true, sync));
  }

  /**
   * Send wechat notification, synchronized if sync is absent.
   */
  public Result send(Wechat wechat, boolean... sync) {
    return service.send(wechat, boolToInt(true, sync));
  }

  private int boolToInt(boolean defaultVal, boolean[] vals) {
    boolean bool = (vals != null && vals.length > 0) ? vals[0] : defaultVal;
    return bool ? 1 : 0;
  }
}
